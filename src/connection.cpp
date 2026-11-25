#include "cpp11/environment.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "r_progress_bar_display.hpp"
#include "rapi.hpp"

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

// Avoid clash with TRUE and FALSE macros in older rtools
#undef TRUE
#undef FALSE

using namespace duckdb;

void duckdb::ConnDeleter(ConnWrapper *conn) {
	cpp11::warning("Connection is garbage-collected, use dbDisconnect() to avoid this.");
	delete conn;
}

// The callbacks of displays destroyed off R's thread, where R must not be called:
// the next display, built on R's thread, releases them.
static std::mutex orphaned_callbacks_lock;
static std::vector<SEXP> orphaned_callbacks;

static void ReleaseOrphanedCallbacks() {
	std::vector<SEXP> callbacks;
	{
		std::lock_guard<std::mutex> guard(orphaned_callbacks_lock);
		callbacks.swap(orphaned_callbacks);
	}
	for (auto callback : callbacks) {
		R_ReleaseObject(callback);
	}
}

unique_ptr<ProgressBarDisplay> RProgressBarDisplay::Create() {
	return make_uniq<RProgressBarDisplay>();
}

void RProgressBarDisplay::Initialize() {
	// Reached from ClientContext::PendingPreparedStatementInternal(), which
	// constructs the ProgressBar -- and with it this display -- while it holds
	// the client context lock. Looking the callback up evaluates R code, and
	// with it a garbage collection.
	RCallbackScope callback_scope;

	cpp11::function getNamespace = RStrings::get().getNamespace_sym;
	cpp11::environment duckdb_namespace(getNamespace(RStrings::get().duckdb_str));
	cpp11::sexp get_progress_display(Rf_lang1(RStrings::get().get_progress_display_sym));
	auto progress_display = cpp11::safe[Rf_eval](get_progress_display, duckdb_namespace);

	if (Rf_isFunction(progress_display)) {
		R_PreserveObject(progress_display);
		progress_callback = progress_display;
	}
}

// The engine builds the display when a query starts, on the thread that issues it. That is R's for a DBI
// call. For a query issued on another thread, R's thread looks the callback up when it serves calls, and
// the display stays silent otherwise.
RProgressBarDisplay::RProgressBarDisplay() : ProgressBarDisplay() {
	char error[1];
	RunOnRThread(
	    [](void *display, char *, size_t) {
		    try {
			    ReleaseOrphanedCallbacks();
			    static_cast<RProgressBarDisplay *>(display)->Initialize();
		    } catch (...) {
			    return 1;
		    }
		    return 0;
	    },
	    this, error, sizeof(error));
}

RProgressBarDisplay::~RProgressBarDisplay() {
	if (progress_callback == R_NilValue) {
		return;
	}
	if (OnRThread()) {
		R_ReleaseObject(progress_callback);
		return;
	}
	std::lock_guard<std::mutex> guard(orphaned_callbacks_lock);
	orphaned_callbacks.push_back(progress_callback);
}

void RProgressBarDisplay::Update(double percentage) {
	// The engine updates the display from whichever thread fetches a streaming result,
	// a thread of arrow's pool among them, and R runs on its own thread only.
	if (progress_callback == R_NilValue || !OnRThread()) {
		return;
	}

	// Called from the executor while the query that drives it holds the lock.
	RCallbackScope callback_scope;

	try {
		cpp11::sexp call = Rf_lang2(progress_callback, Rf_ScalarReal(percentage));
		cpp11::safe[Rf_eval](call, R_BaseEnv);
	} catch (std::exception &e) {
		// Ignore progress bar error
	}
}

void RProgressBarDisplay::Finish() {
	Update(100);
}

static void SetDefaultConfigArguments(ClientContext &context) {
	auto &config = ClientConfig::GetConfig(context);
	// Set the function used to create the display for the progress bar
	config.display_create_func = RProgressBarDisplay::Create;
	config.enable_progress_bar = true;
	config.wait_time = 0;
}

[[cpp11::register]] duckdb::conn_eptr_t rapi_connect(duckdb::db_eptr_t dual, duckdb::ConvertOpts convert_opts) {
	if (!dual || !dual.get()) {
		rapi_error_with_context("rapi_connect", "Invalid database reference");
	}
	auto db = dual->get();
	if (!db || !db->db) {
		rapi_error_with_context("rapi_connect", "Database already closed");
	}

	auto conn_wrapper = make_uniq<ConnWrapper>(std::move(db), std::move(convert_opts));

	// Set progress display config
	auto &client_context = *conn_wrapper->conn->context;
	SetDefaultConfigArguments(client_context);

	// The connection now holds a reference to the database.
	// This reference is released when the connection is closed.
	// From the R side, the database pointer will remain valid
	// as long as at least one connection to that database is open.
	dual->unlock();

	return conn_eptr_t(conn_wrapper.release());
}

[[cpp11::register]] void rapi_disconnect(duckdb::conn_eptr_t conn) {
	// An object parked during the last R callback holds its client context, and with it the database:
	// destroy it before letting go of the connection, or the file stays open (#2934).
	// A no-op inside a callback and under a running query, as everywhere: a finalizer may disconnect there.
	RCallbackScope::Drain();
	auto conn_wrapper = conn.release();
	if (conn_wrapper) {
		delete conn_wrapper;
	}
}

// Whether a transaction begun by BEGIN is open: a statement's own transaction,
// and the one a streaming result holds, are not.
[[cpp11::register]] bool rapi_connection_in_transaction(duckdb::conn_eptr_t conn) {
	if (!conn || !conn.get() || !conn->conn) {
		rapi_error_with_context("rapi_connection_in_transaction", "Invalid connection");
	}
	return !conn->conn->IsAutoCommit();
}

// Test-only, unexported: runs a query on a thread of its own, so the query binds, and runs, off R's thread
// every time (#2962). R's thread only waits for it, or with options(duckdb.serve_r_calls = TRUE) serves the
// calls that thread queues for R.
// Returns the error message, or NA.
[[cpp11::register]] SEXP rapi_query_off_r_thread(duckdb::conn_eptr_t conn, std::string sql) {
	if (!conn || !conn.get() || !conn->conn) {
		rapi_error_with_context("rapi_query_off_r_thread", "Invalid connection");
	}
	auto &connection = *conn->conn;
	bool failed = false;
	string message;
	std::atomic<bool> finished {false};
	// Serving starts before the thread does, so its first call for R finds a server
	duckdb::unique_ptr<RThreadServer> server;
	if (ServeRCallsOption()) {
		server = make_uniq<RThreadServer>();
	}
	std::thread worker([&]() {
		try {
			auto result = connection.Query(sql);
			if (result->HasError()) {
				failed = true;
				message = result->GetError();
			}
		} catch (std::exception &ex) {
			failed = true;
			message = ErrorData(ex).Message();
		} catch (...) {
			// An exception escaping a std::thread would terminate R
			failed = true;
			message = "unknown error";
		}
		finished = true;
	});
	while (server && !finished) {
		ServeRThreadCalls(1);
	}
	worker.join();
	if (!failed) {
		return Rf_ScalarString(NA_STRING);
	}
	return Rf_mkString(message.c_str());
}

// Replaces the policy that decides which errors abort the open transaction,
// and returns the one it replaced, so that the caller can put it back as it was.
// Set directly, as BEGIN sets it, rather than through SET, which a locked
// configuration refuses.
[[cpp11::register]] std::string rapi_connection_set_invalidation_policy(duckdb::conn_eptr_t conn, std::string policy) {
	if (!conn || !conn.get() || !conn->conn) {
		rapi_error_with_context("rapi_connection_set_invalidation_policy", "Invalid connection");
	}
	auto &transaction = conn->conn->context->transaction;
	auto previous = EnumUtil::ToString(transaction.GetInvalidationPolicy());
	transaction.SetInvalidationPolicy(EnumUtil::FromString<TransactionInvalidationPolicy>(policy));
	return previous;
}

[[cpp11::register]] bool rapi_connection_valid(duckdb::conn_eptr_t conn) {
	// Check connection validity without acquiring ClientContext locks
	// This avoids the "ScopedInterruptHandler already active" issue when
	// called from progress bar handlers or other contexts that already
	// have interrupt protection
	if (!conn || !conn.get()) {
		return false;
	}

	auto conn_wrapper = conn.get();
	if (!conn_wrapper) {
		return false;
	}

	// Check if the connection object exists
	if (!conn_wrapper->conn) {
		return false;
	}

	// Check if the database wrapper is still valid
	if (!conn_wrapper->db || !conn_wrapper->db->db) {
		return false;
	}

	// All checks passed - connection appears valid
	return true;
}

// Test-only, unexported: registers `duckdb_r_test_call()`, a scalar function that calls the R function
// in `options(duckdb.test_call)` through RunOnRThread(). It stands in for an extension whose functions
// run R code under a running query, outside any callback of this package, from any engine thread.
[[cpp11::register]] void rapi_register_test_call(duckdb::conn_eptr_t conn) {
	if (!conn || !conn.get() || !conn->conn) {
		rapi_error_with_context("rapi_register_test_call", "Invalid connection");
	}
	ScalarFunction function("duckdb_r_test_call", {}, LogicalType::INTEGER,
	                        [](DataChunk &args, ExpressionState &state, Vector &result) {
		                        // Any thread: R's thread runs the R function, inline or when it serves calls.
		                        // A failed or interrupted query withdraws the call: R's thread then waits for this task.
		                        auto &context = state.GetContext();
		                        char error[256];
		                        auto status = RunOnRThread(
		                            [](void *, char *err, size_t cap) {
			                            SEXP fn = Rf_GetOption1(Rf_install("duckdb.test_call"));
			                            if (!Rf_isFunction(fn)) {
				                            return 0;
			                            }
			                            SEXP call = PROTECT(Rf_lang1(fn));
			                            int failed = 0;
			                            R_tryEvalSilent(call, R_GlobalEnv, &failed);
			                            UNPROTECT(1);
			                            if (failed) {
				                            std::snprintf(err, cap, "%s", R_curErrorBuf());
			                            }
			                            return failed;
		                            },
		                            nullptr, error, sizeof(error),
		                            [](void *context_p) -> int {
		                                auto &query = *static_cast<ClientContext *>(context_p);
		                                return query.IsInterrupted() || query.GetExecutor().HasError();
		                            },
		                            &context);
		                        if (status != 0) {
			                        throw InvalidInputException("duckdb_r_test_call(): %s", error);
		                        }
		                        result.Reference(Value::INTEGER(1), count_t(args.size()));
	                        });
	function.SetVolatile();
	function.SetFallible();
	CreateScalarFunctionInfo info(std::move(function));
	conn->conn->context->RegisterFunction(info);
}
