#!/usr/bin/env Rscript

cat("# join-arrow\n")

source("./_helpers/helpers.R")

.libPaths("./arrow/r-arrow") # tidyverse/dplyr#4641 ## leave it like here in case if this affects arrow pkg as well
suppressPackageStartupMessages({
  library("arrow", lib.loc="./arrow/r-arrow", warn.conflicts=FALSE)
  library("dplyr", lib.loc="./arrow/r-arrow", warn.conflicts=FALSE)
})
ver = packageVersion("arrow")
git = ""
task = "join"
solution = "arrow"
cache = TRUE
on_disk = FALSE


x = read_csv_arrow("../data/J1_1e7_NA_0_0.csv", schema = schema(id1=int32(),id2=int32(),id3=int32(),id4=dictionary(),id5=dictionary(),id6=dictionary(),v1=double()), skip=1, as_data_frame=FALSE)
small = read_csv_arrow("../data/J1_1e7_1e1_0_0.csv", schema = schema(id1=int32(),id4=dictionary(),v2=double()), skip=1, as_data_frame=FALSE)
medium = read_csv_arrow("../data/J1_1e7_1e4_0_0.csv", schema = schema(id1=int32(),id2=int32(),id4=dictionary(),id5=dictionary(),v2=double()), skip=1, as_data_frame=FALSE)
big = read_csv_arrow("../data/J1_1e7_1e7_0_0.csv", schema = schema(id1=int32(),id2=int32(),id3=int32(),id4=dictionary(),id5=dictionary(),id6=dictionary(),v2=double()), skip=1, as_data_frame=FALSE)


task_init = proc.time()[["elapsed"]]
cat("joining...\n")

question = "small inner on int" # q1
fun = "inner_join"
t = system.time(print(dim(ans<-inner_join(x, small, by="id1"))))[["elapsed"]]
rm(ans)
print(t)

t = system.time(print(dim(ans<-inner_join(x, small, by="id1"))))[["elapsed"]]
rm(ans)
print(t)

question = "medium inner on int" # q2
fun = "inner_join"
t = system.time(print(dim(ans<-inner_join(x, medium, by="id2"))))[["elapsed"]]
rm(ans)
print(t)

t = system.time(print(dim(ans<-inner_join(x, medium, by="id2"))))[["elapsed"]]
rm(ans)
print(t)

question = "medium outer on int" # q3
fun = "left_join"
t = system.time(print(dim(ans<-left_join(x, medium, by="id2"))))[["elapsed"]]
rm(ans)
print(t)

t = system.time(print(dim(ans<-left_join(x, medium, by="id2"))))[["elapsed"]]
rm(ans)
print(t)

question = "medium inner on factor" # q4
fun = "inner_join"
t = system.time(print(dim(ans<-inner_join(x, medium, by="id5"))))[["elapsed"]]
rm(ans)
print(t)

t = system.time(print(dim(ans<-inner_join(x, medium, by="id5"))))[["elapsed"]]
rm(ans)
print(t)

question = "big inner on int" # q5
fun = "inner_join"
t = system.time(print(dim(ans<-inner_join(x, big, by="id3"))))[["elapsed"]]
rm(ans)
print(t)

t = system.time(print(dim(ans<-inner_join(x, big, by="id3"))))[["elapsed"]]
rm(ans)
print(t)
