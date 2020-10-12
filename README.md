# Milestone 1

In this stage, since we only need to use one map worker and one reduce worker,
we call `mapWorker` once to produce intermediate data and call `reduceWorker` once to process intermediate data and output result to a file.

When `mapWorker` is working, it calls the user defined function `mapFunction` to implement specific requirements;
Similarly, `reduceWorker` calls UDF `reduceFunction`.

Example for running word count application:

`$ cd src/main`

`$ go run map_reduce_single.go hamlet.txt `

result will be stored in the output file


