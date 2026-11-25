# Demo of the duckdb_r_api_v1 proof of concept. Run under a timeout with the
# PoC duckdb first on the library path; args: path to the compiled client.
suppressPackageStartupMessages(library(DBI))
loadNamespace("duckdb")
say <- function(...) { cat(..., "\n"); flush.console() }
say("duckdb", as.character(packageVersion("duckdb")), "from", dirname(find.package("duckdb")))
dyn.load(commandArgs(trailingOnly = TRUE)[[1L]])
demo <- function(fn, threads, calls, serve) {
  out <- .Call("demo_run", fn, as.integer(threads), as.integer(calls), serve)
  setNames(out, c("api_version", "ok", "failed", "not_served"))
}

say("\n== 1. Native threads run an R function through run_on_r_thread()")
seen <- integer()
r_pid <- Sys.getpid()
print(demo(function(id) { stopifnot(Sys.getpid() == r_pid); seen <<- c(seen, id) }, 4, 250, TRUE))
say("R calls made:", length(seen), "| per thread:", paste(tabulate(seen, 4), collapse = " "))

say("\n== 2. An R error stays inside the call")
print(demo(function(id) stop("boom"), 2, 5, TRUE))

say("\n== 3. R's thread does not serve: calls fail at once, nothing waits")
print(demo(function(id) NULL, 4, 250, FALSE))

con <- dbConnect(duckdb::duckdb(shared_home = FALSE))
off_thread <- function(sql) duckdb:::query_off_r_thread(con, sql)

say("\n== 4. duckdb's data frame bind of an ALTREP list column, query on a thread that is not R's")
lazy <- local({
  con_rel <- dbConnect(duckdb::duckdb(shared_home = FALSE))
  duckdb:::rel_to_altrep(duckdb:::rel_from_sql(con_rel, "SELECT * FROM (VALUES (1, [1, 2]), (2, [3])) t(a, l)"))
})
duckdb::duckdb_register(con, "lazy_view", lazy)
say("without serving (as today):", substr(format(off_thread("SELECT a FROM lazy_view")), 1, 76))
options(duckdb.serve_r_calls = TRUE)
say("with serving, NA = no error:", format(off_thread("SELECT a FROM lazy_view")))
options(duckdb.serve_r_calls = NULL)

say("\n== 5. options(duckdb.serve_r_calls = TRUE): dbGetQuery() serves between tasks")
options(duckdb.serve_r_calls = TRUE)
dbExecute(con, "SET threads = 4")
dbExecute(con, "CREATE TABLE big AS SELECT i, i % 7 AS g FROM range(2000000) t(i)")
print(dbGetQuery(con, "SELECT g, count(*) AS n, sum(i) AS s FROM big GROUP BY g ORDER BY g LIMIT 3"))
say("rows through a registered data frame:", dbGetQuery(con, "SELECT count(*) AS n FROM lazy_view JOIN pragma_table_info('lazy_view') m ON true")$n)
say("error is reported                    :", tryCatch(dbGetQuery(con, "SELECT nope FROM big"), error = function(e) "yes"))
say("\n== 6. An R function as a scalar function on 4 engine threads")
duckdb:::register_test_call(con)
calls <- 0L
options(duckdb.test_call = function() calls <<- calls + 1L)
print(dbGetQuery(con, "SELECT sum(duckdb_r_test_call()) AS s FROM big"))
say("R calls, all on R's thread:", calls)
options(duckdb.serve_r_calls = NULL)
say("without serving:", tryCatch(dbGetQuery(con, "SELECT sum(duckdb_r_test_call()) AS s FROM big"), error = function(e) substr(conditionMessage(e), 1, 90)))
dbDisconnect(con)
say("\nDONE")
