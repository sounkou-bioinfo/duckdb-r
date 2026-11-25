# duckdb_r_api_v1 demo

Proof of concept for `inst/include/duckdb_r_api.h`: code on a thread other than
R's asks R's thread to run a call, and blocks until it ran.

```sh
R CMD INSTALL .                      # this branch
cd scripts/r-api-demo
PKG_CPPFLAGS=-I$(Rscript -e 'cat(system.file("include", package = "duckdb"))') \
  PKG_LIBS=-lpthread R CMD SHLIB -o client.so client.c
Rscript demo.R "$PWD/client.so"
```

`client.c` stands in for a DuckDB extension: its native threads call
`run_on_r_thread()` with a C function that evaluates an R function.
`demo.R` shows the calls served by a client loop, an R error contained in the
call, calls refused while R's thread does not serve, the data frame bind of an
ALTREP column routed to R's thread, and `dbGetQuery()` serving between engine tasks under
`options(duckdb.serve_r_calls = TRUE)`, and an R function that runs as a scalar
function on four engine threads.
