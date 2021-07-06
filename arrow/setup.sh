#!/bin/bash
set -e

# install stable arrow
mkdir -p ./arrow/r-arrow

Rscript -e 'Sys.setenv(ARROW_WITH_GZIP = "ON") install.packages(c("arrow","dplyr"), lib="./arrow/r-arrow")'