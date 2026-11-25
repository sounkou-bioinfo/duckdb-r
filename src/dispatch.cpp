// Runs calls from other threads on R's thread (inst/include/duckdb_r_api.h).
//
// A thread that needs R queues a call and blocks. R's thread runs the queued
// calls while it serves: during a query executed with
// options(duckdb.serve_r_calls = TRUE), or inside a client's own serve loop.
// A call queued while R's thread does not serve fails at once, so no caller
// waits for a thread that will not come.
#include "rapi.hpp"

#include "../inst/include/duckdb_r_api.h"

#include <R_ext/Rdynload.h>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <algorithm>
#include <deque>
#include <mutex>

using namespace duckdb;

namespace {

struct RCall {
	duckdb_r_call_fn fn;
	void *data;
	char *err;
	size_t cap;
	int status;
	bool done;
};

// How often a caller that waits for R's thread asks whether its query was cancelled
const int CANCEL_CHECK_MS = 5;

std::mutex calls_mutex;
std::condition_variable call_posted;
std::condition_variable call_finished;
std::deque<RCall *> pending_calls;
idx_t servers = 0;

void SetCallError(RCall &call, const char *message) {
	if (call.err && call.cap > 0) {
		std::snprintf(call.err, call.cap, "%s", message);
	}
}

// A C++ exception must not cross R's frames, nor the caller's C frames.
int InvokeCall(RCall &call) noexcept {
	try {
		return call.fn(call.data, call.err, call.cap);
	} catch (std::exception &ex) {
		SetCallError(call, ex.what());
	} catch (...) {
		SetCallError(call, "The call on R's thread was interrupted");
	}
	return DUCKDB_R_CALL_FAILED;
}

void InvokeCallToplevel(void *call_p) {
	auto &call = *static_cast<RCall *>(call_p);
	call.status = InvokeCall(call);
}

} // namespace

bool duckdb::RThreadServing() {
	std::lock_guard<std::mutex> guard(calls_mutex);
	return servers > 0;
}

RThreadServer::RThreadServer() {
	std::lock_guard<std::mutex> guard(calls_mutex);
	servers++;
}

RThreadServer::~RThreadServer() {
	std::lock_guard<std::mutex> guard(calls_mutex);
	servers--;
	if (servers > 0) {
		return;
	}
	for (auto call : pending_calls) {
		SetCallError(*call, "R's thread stopped serving calls");
		call->status = DUCKDB_R_CALL_NOT_SERVED;
		call->done = true;
	}
	pending_calls.clear();
	call_finished.notify_all();
}

idx_t duckdb::ServeRThreadCalls(int wait_ms) {
	idx_t served = 0;
	std::unique_lock<std::mutex> lock(calls_mutex);
	if (pending_calls.empty() && wait_ms > 0) {
		call_posted.wait_for(lock, std::chrono::milliseconds(wait_ms), [] { return !pending_calls.empty(); });
	}
	while (!pending_calls.empty()) {
		auto call = pending_calls.front();
		pending_calls.pop_front();
		lock.unlock();
		// The status stays "failed" when an R error or interrupt jumps out of the call
		call->status = DUCKDB_R_CALL_FAILED;
		if (!R_ToplevelExec(InvokeCallToplevel, call) && call->err && call->cap > 0 && call->err[0] == '\0') {
			SetCallError(*call, "An R error or interrupt ended the call on R's thread");
		}
		lock.lock();
		call->done = true;
		call_finished.notify_all();
		served++;
	}
	return served;
}

int duckdb::RunOnRThread(duckdb_r_call_fn fn, void *data, char *err, size_t cap, duckdb_r_cancelled_fn cancelled,
                         void *cancel_data) {
	if (err && cap > 0) {
		err[0] = '\0';
	}
	RCall call {fn, data, err, cap, DUCKDB_R_CALL_FAILED, false};
	if (OnRThread()) {
		return InvokeCall(call);
	}
	std::unique_lock<std::mutex> lock(calls_mutex);
	if (servers == 0) {
		SetCallError(call, "R's thread does not serve calls now");
		return DUCKDB_R_CALL_NOT_SERVED;
	}
	pending_calls.push_back(&call);
	call_posted.notify_one();
	if (!cancelled) {
		call_finished.wait(lock, [&] { return call.done; });
		return call.status;
	}
	// R's thread may be inside the engine, waiting for this very thread to end its task: that happens when
	// the query is cancelled or has failed. A call that R's thread has not started is then withdrawn.
	while (!call_finished.wait_for(lock, std::chrono::milliseconds(CANCEL_CHECK_MS), [&] { return call.done; })) {
		lock.unlock();
		auto stop = cancelled(cancel_data) != 0;
		lock.lock();
		if (!stop || call.done) {
			continue;
		}
		auto queued = std::find(pending_calls.begin(), pending_calls.end(), &call);
		if (queued != pending_calls.end()) {
			pending_calls.erase(queued);
			SetCallError(call, "The query was cancelled before R's thread ran the call");
			return DUCKDB_R_CALL_CANCELLED;
		}
		// R's thread runs the call now, and completes it
	}
	return call.status;
}

namespace {

// One server for a client's serve_begin()/serve_end() bracket
RThreadServer *client_server = nullptr;

void ApiServeBegin() {
	if (OnRThread() && !client_server) {
		client_server = new RThreadServer();
	}
}

int ApiServe(int wait_ms) {
	if (!OnRThread() || !client_server) {
		return -1;
	}
	return static_cast<int>(ServeRThreadCalls(wait_ms));
}

void ApiServeEnd() {
	if (OnRThread() && client_server) {
		delete client_server;
		client_server = nullptr;
	}
}

int ApiRunOnRThread(duckdb_r_call_fn fn, void *data, char *err, size_t cap) {
	return RunOnRThread(fn, data, err, cap);
}

const duckdb_r_api_v1 r_api_v1 = {1, ApiRunOnRThread, RunOnRThread, ApiServeBegin, ApiServe, ApiServeEnd};

const duckdb_r_api_v1 *GetRApiV1() {
	return &r_api_v1;
}

} // namespace

void duckdb::RegisterRApi() {
	R_RegisterCCallable("duckdb", "duckdb_r_api_v1", reinterpret_cast<DL_FUNC>(GetRApiV1));
}
