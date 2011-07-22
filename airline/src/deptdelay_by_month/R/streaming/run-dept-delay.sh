hadoop jar /usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-0.20.2-CDH3B4.jar -input /data/airline/1988.csv -output /output -mapper map.R -reducer reduce.R -file map.R -file reduce.R
