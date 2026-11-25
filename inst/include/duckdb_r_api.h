/* Native entry points of the duckdb R package for code that runs off R's
 * thread: DuckDB extensions and packages whose native threads need R.
 *
 *   typedef const struct duckdb_r_api_v1 *(*duckdb_r_api_v1_fn)(void);
 *   const struct duckdb_r_api_v1 *api =
 *       ((duckdb_r_api_v1_fn) R_GetCCallable("duckdb", "duckdb_r_api_v1"))();
 *
 * Proof of concept. Not a stable interface.
 */
#ifndef DUCKDB_R_API_H
#define DUCKDB_R_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A call that runs on R's thread and may use the R API. It returns 0 on
 * success. On failure it returns nonzero and writes a message to err. It must
 * contain its R errors (R_tryCatchError, R_UnwindProtect): a jump out of the
 * call is reported as a failure without a message of its own. */
typedef int (*duckdb_r_call_fn)(void *data, char *err, size_t cap);

#define DUCKDB_R_CALL_OK 0
#define DUCKDB_R_CALL_FAILED 1
/* R's thread does not serve calls now; the call did not run. */
#define DUCKDB_R_CALL_NOT_SERVED 2
/* The caller's query was cancelled before R's thread started the call; it did not run. */
#define DUCKDB_R_CALL_CANCELLED 3

/* Any thread. Returns nonzero when the work that waits for the call is cancelled. */
typedef int (*duckdb_r_cancelled_fn)(void *cancel_data);

struct duckdb_r_api_v1 {
	uint32_t version;

	/* Any thread. On R's thread the call runs inline. On another thread it is
	 * queued, and the caller blocks until R's thread ran it. Returns one of
	 * the DUCKDB_R_CALL_* codes. */
	int (*run_on_r_thread)(duckdb_r_call_fn fn, void *data, char *err, size_t cap);

	/* The same, for a caller inside a query. R's thread can be inside the engine,
	 * waiting for the caller's task to end: this happens when the query failed
	 * or is interrupted. The waiting thread therefore asks cancelled() every few
	 * ms, and withdraws a call that R's thread has not started. A call that
	 * runs is always completed. */
	int (*run_on_r_thread_cancellable)(duckdb_r_call_fn fn, void *data, char *err, size_t cap,
	                                   duckdb_r_cancelled_fn cancelled, void *cancel_data);

	/* R's thread only. duckdb serves queued calls while it executes a query
	 * with options(duckdb.serve_r_calls = TRUE). A package that keeps R's
	 * thread in a loop of its own serves them itself: between serve_begin()
	 * and serve_end(), serve() runs the queued calls and returns their count,
	 * after waiting up to wait_ms for the first one. */
	void (*serve_begin)(void);
	int (*serve)(int wait_ms);
	void (*serve_end)(void);
};

typedef const struct duckdb_r_api_v1 *(*duckdb_r_api_v1_fn)(void);

#ifdef __cplusplus
}
#endif

#endif
