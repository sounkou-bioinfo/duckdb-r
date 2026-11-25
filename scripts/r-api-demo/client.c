/* Demo client of duckdb_r_api_v1: native threads ask R's thread to run an R
 * function. Stands in for a DuckDB extension whose worker threads need R. */
#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>

#include "duckdb_r_api.h"

struct job {
	const struct duckdb_r_api_v1 *api;
	SEXP fn; /* kept alive by the caller on R's thread */
	int thread_id;
	int calls;
	int ok;
	int failed;
	int not_served;
	atomic_int *finished;
};

/* The next three functions run on R's thread. */
static SEXP eval_body(void *data) {
	struct job *job = data;
	SEXP call = PROTECT(Rf_lang2(job->fn, Rf_ScalarInteger(job->thread_id)));
	Rf_eval(call, R_GlobalEnv);
	UNPROTECT(1);
	return R_NilValue;
}

static SEXP eval_error(SEXP cond, void *data) {
	*(int *) data = 1;
	return R_NilValue;
}

static int call_r(void *data, char *err, size_t cap) {
	int failed = 0;
	R_tryCatchError(eval_body, data, eval_error, &failed);
	if (failed) snprintf(err, cap, "the R function failed");
	return failed ? DUCKDB_R_CALL_FAILED : DUCKDB_R_CALL_OK;
}

/* Runs on a native thread: no R API here. */
static void *worker(void *data) {
	struct job *job = data;
	char err[256];
	for (int i = 0; i < job->calls; i++) {
		int status = job->api->run_on_r_thread(call_r, job, err, sizeof(err));
		if (status == DUCKDB_R_CALL_OK) job->ok++;
		else if (status == DUCKDB_R_CALL_NOT_SERVED) job->not_served++;
		else job->failed++;
	}
	atomic_fetch_add(job->finished, 1);
	return NULL;
}

SEXP demo_run(SEXP fn, SEXP n_threads_sexp, SEXP n_calls_sexp, SEXP serve_sexp) {
	const struct duckdb_r_api_v1 *api =
	    ((duckdb_r_api_v1_fn) R_GetCCallable("duckdb", "duckdb_r_api_v1"))();
	int n_threads = Rf_asInteger(n_threads_sexp);
	int serve = Rf_asLogical(serve_sexp);
	atomic_int finished = 0;
	pthread_t *threads = (pthread_t *) R_alloc(n_threads, sizeof(pthread_t));
	struct job *jobs = (struct job *) R_alloc(n_threads, sizeof(struct job));

	if (serve) api->serve_begin();
	for (int i = 0; i < n_threads; i++) {
		jobs[i] = (struct job) {api, fn, i + 1, Rf_asInteger(n_calls_sexp), 0, 0, 0, &finished};
		pthread_create(&threads[i], NULL, worker, &jobs[i]);
	}
	while (serve && atomic_load(&finished) < n_threads) {
		api->serve(5);
	}
	for (int i = 0; i < n_threads; i++) pthread_join(threads[i], NULL);
	if (serve) api->serve_end();

	SEXP out = PROTECT(Rf_allocVector(INTSXP, 4));
	INTEGER(out)[0] = (int) api->version;
	INTEGER(out)[1] = INTEGER(out)[2] = INTEGER(out)[3] = 0;
	for (int i = 0; i < n_threads; i++) {
		INTEGER(out)[1] += jobs[i].ok;
		INTEGER(out)[2] += jobs[i].failed;
		INTEGER(out)[3] += jobs[i].not_served;
	}
	UNPROTECT(1);
	return out;
}
