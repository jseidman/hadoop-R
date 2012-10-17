#!/usr/bin/env Rscript

# Calculate average departure delays by year and month for each airline in the
# airline data set (http://stat-computing.org/dataexpo/2009/the-data.html).
# Requires rmr package (https://github.com/RevolutionAnalytics/RHadoop/wiki).

#
# This file is updated to work with the new version 1.2 of rmr:
# 
# Lots changed in rmr 1.2. The focus of the release was on adding flexibility
# to the I/O, such as adding support for binary files, etc.
# Partially as a result, there are some incompatibilities in calling mapreduce(),
# particularly related to I/O. Relevant changes are noted below.
# 
# --jbreen 2/28/12
#

library(rmr)

# first, let's adapt our input function to look like one from make.input.format()
# (using the result of make.input.format('csv') would require changes to our mapper )

csvtextinputformat = list(mode = 'text', format = function(line) {
								keyval(NULL, unlist(strsplit(line, "\\,")))
							}, streaming.format=NULL)

deptdelay = function (input, output) {
  mapreduce(input = input,
            output = output,
            input.format = csvtextinputformat,
            map = function(k, fields) {
              # Skip header lines and bad records:
              if (!(identical(fields[[1]], "Year")) & length(fields) == 29) {
                deptDelay <- fields[[16]]
                # Skip records where departure dalay is "NA":
                if (!(identical(deptDelay, "NA"))) {
                  # field[9] is carrier, field[1] is year, field[2] is month:
                  keyval(c(fields[[9]], fields[[1]], fields[[2]]), deptDelay)
                }
              }
            },
            reduce = function(keySplit, vv) {
              keyval(keySplit[[2]], c(keySplit[[3]], length(vv), keySplit[[1]], mean(as.numeric(vv))))
            })
}

df = from.dfs(deptdelay("/data/airline/", "/dept-delay-month-rmr12"), to.data.frame=T)
colnames(df) = c('year', 'month', 'count', 'airline', 'mean.delay')
print(df)
