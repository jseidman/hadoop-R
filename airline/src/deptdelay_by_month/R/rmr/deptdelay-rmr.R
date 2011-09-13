#!/usr/bin/env Rscript

# Calculate average departure delays by year and month for each airline in the
# airline data set (http://stat-computing.org/dataexpo/2009/the-data.html).
# Requires rmr package (https://github.com/RevolutionAnalytics/RHadoop/wiki).

library(rmr)

deptdelay = function (input, output) {
  mapreduce(input = input,
            output = output,
            textinputformat = rawtextinputformat,
            map = function(k, v) {
              fields <- unlist(strsplit(v, "\\,"))
              # Skip header lines and bad records:
              if (!(identical(fields[[1]], "Year")) & length(fields) == 29) {
                deptDelay <- fields[[16]]
                # Skip records where departure dalay is "NA":
                if (!(identical(deptDelay, "NA"))) {
                  # field[9] is carrier, field[1] is year, field[2] is month:
                  keyval(paste(fields[[9]], "|", fields[[1]], "|", fields[[2]],
                         sep=""),
                         deptDelay)
                }
              }
            },
            reduce = function(k, vv) {
              keySplit <- unlist(strsplit(k, "\\|"))
              keyval(keySplit[[2]], 
                     paste(keySplit[[3]], length(vv), keySplit[[1]],
                           mean(as.numeric(vv)), sep="\t"))
            })
}

from.dfs(deptdelay("/data/airline/", "/dept-delay-month"))
