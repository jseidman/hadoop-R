#! /usr/bin/env Rscript

library(Rhipe)
rhinit(TRUE, TRUE)

map <- expression({
  # For each input record, parse out required fields and output new record:
  mapLine = function(line) {
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
  # Process each record in map input:
  lapply(map.values, mapLine)
})

reduce <- expression(
  reduce = {
    count <- length(reduce.values)
    avg <- mean(as.numeric(reduce.values))
    keySplit <- unlist(strsplit(reduce.key, "\\|"))
  },
  post = {
    rhcollect(keySplit[[2]], 
              paste(keySplit[[3]], count, keySplit[[1]], avg, sep="\t"))
  }
)

# Create job object:
z <- rhmr(map=map, reduce=reduce,
          ifolder="/data/airline/", ofolder="/dept-delay-month",
          inout=c('text', 'text'), jobname='Avg Departure Delay By Month')
# Run it:
rhex(z)

# Visualize results:
library(lattice)
rhget("/dept-delay-month/part-r-00000", "deptdelay.dat")
deptdelays.monthly.full <- read.delim("deptdelay.dat", header=F)
names(deptdelays.monthly.full)<- c("Year","Month","Count","Airline","Delay")
deptdelays.monthly.full$Year <- as.character(deptdelays.monthly.full$Year)
h <- histogram(~Delay|Year,data=deptdelays.monthly.full,layout=c(5,5))
update(h)
