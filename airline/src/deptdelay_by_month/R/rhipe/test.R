#! /usr/bin/env Rscript

library(Rhipe)
rhinit(TRUE, TRUE)

map <- expression({
  mapLine = function(line) {
    cat("line=", line, "\n")
    fields <- unlist(strsplit(line, "\\,"))
    # Skip header lines and bad records:
    if (!(identical(fields[[1]], "Year")) & length(fields) == 29) {
      deptDelay <- fields[[16]]
     # Skip records where departure dalay is "NA":
      if (!(identical(deptDelay, "NA"))) {
        # field[9] is carrier, field[1] is year, field[2] is month:
        rhcollect(paste(fields[[9]], "|", fields[[1]], "|", fields[[2]], sep=""), deptDelay)
      }
    }
  }
  lapply(map.values, mapLine)
})

## Create a job object
z <- rhmr(map=map,
          ifolder="/data/airline/test.dat", ofolder="/tmp/test",
          inout=c('text', 'text'), jobname='test',mapred=list(mapred.reduce.tasks=0))
rhex(z)
