#!/usr/bin/env Rscript

cat("# groupby-arrow\n")

# source("./_helpers/helpers.R")

stopifnot(requireNamespace("bit64", quietly=TRUE)) # used in chk to sum numeric columns
# .libPaths("./arrow/r-arrow") # tidyverse/dplyr#4641 ## leave it like here in case if this affects arrow pkg as well
# suppressPackageStartupMessages({
#   library("arrow", lib.loc="./arrow/r-arrow", warn.conflicts=FALSE)
#   library("dplyr", lib.loc="./arrow/r-arrow", warn.conflicts=FALSE)
# })

library("arrow")
library("dplyr")



library("purrr")
library("duckdb")
library("DBI")

con <- dbConnect(duckdb::duckdb())
#dbExecute(con, "PRAGMA threads=8")


## patch the arrow group by function ehehehe
group_by.arrow_dplyr_query <- function(.data,
                                       ...,
                                       .add = FALSE,
                                       add = .add,
                                       .drop = dplyr::group_by_drop_default(.data)) {
  .data = arrow:::arrow_dplyr_query(.data)
  groups = vapply(enquos(...), rlang::quo_name, "character")
  # TODO Actually create duckdb grouped relation here
  return(structure(list(data=.data, groups = groups), class = "my_duckdb_arrow_grouped_relation"))
}

group_by.Dataset <- group_by.ArrowTabular <- group_by.arrow_dplyr_query

summarise.my_duckdb_arrow_grouped_relation <- function(.data, ...) {
  # TODO better translation of aggregate functions, parse tree traversal
  aggregates = vapply(enquos(...), rlang::quo_name, "character")
  tbl_name = paste0(replicate(10, sample(LETTERS, 1, TRUE)), collapse="")

  duckdb::duckdb_register_arrow(con, tbl_name, .data$data)

  groups_str = paste(.data$groups, collapse=", ")
  aggr_str = paste(aggregates, collapse=", ")
  # TODO use relational API instead of SQL string construction
  res = dbGetQuery(con, sprintf("SELECT %s, %s FROM %s GROUP BY %s", 
    groups_str, aggr_str, tbl_name, groups_str ))
  duckdb::duckdb_unregister_arrow(con, tbl_name)
  return(res)
}


ver = packageVersion("arrow")
git = ""
task = "groupby"
solution = "arrow"
fun = "group_by"
cache = TRUE
on_disk = FALSE

data_name = Sys.getenv("SRC_DATANAME")
# src_grp = file.path("data", paste(data_name, "csv", sep="."))
cat(sprintf("loading dataset %s\n", data_name))

x = read_csv_arrow("../data/G1_1e7_1e2_5_0.csv.gz", schema = schema(id1=string(),id2=string(),id3=string(),id4=int32(),id5=int32(),id6=int32(),v1=int32(),v2=int32(),v3=double()), skip=1, as_data_frame=FALSE)
print(nrow(x))

task_init = proc.time()[["elapsed"]]
cat("grouping...\n")

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-x %>% group_by(id1) %>% summarise(v1=sum(v1)))))[["elapsed"]]
rm(ans)
print(t)

question = "sum v1 by id1" # q1
t = system.time(print(dim(ans<-x %>% group_by(id1) %>% summarise(v1=sum(v1)))))[["elapsed"]]
rm(ans)
print(t)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-x %>% group_by(id1, id2) %>% summarise(v1=sum(v1)))))[["elapsed"]]
rm(ans)
print(t)

question = "sum v1 by id1:id2" # q2
t = system.time(print(dim(ans<-x %>% group_by(id1, id2) %>% summarise(v1=sum(v1)))))[["elapsed"]]
rm(ans)
print(t)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-x %>% group_by(id3) %>% summarise(v1=sum(v1), v3=avg(v3)))))[["elapsed"]]
rm(ans)
print(t)

question = "sum v1 mean v3 by id3" # q3
t = system.time(print(dim(ans<-x %>% group_by(id3) %>% summarise(v1=sum(v1), v3=avg(v3)))))[["elapsed"]]
rm(ans)
print(t)

