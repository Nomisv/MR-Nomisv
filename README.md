# Milestone 2

Group: Xueteng Qian, Haiyang Wu

We have the same code and files.

In this stage, multiple worker processes can communicate with one master process to request tasks and report tasks.

One worker process will continuously communicate with master using RPC until all jobs are done.

Mappers hashes and stores data in intermediate files, and reducers will read from corresponding intermediate files specified by task index and write to corresponding output files.

When there are multiple workers, master uses a mutex lock to ensure safety of data.

Due to some tiny bugs with splitting files, our program works only if the size of the file(in bytes) is divisible by the number of workers.

We will fix this problem soon.

# Run Test

`$ cd src/main`

We firstly start master process in one terminal: (parameters: inputFile, N wokers)

`Terminal 1`

`$ go run master_main.go hamlet.txt 3`

or like

`$ go run master_main.go hamlet.txt 6`

Then run the test in the other terminal:

`Terminal 2` 

`$ sh test.sh`

The test script will start multiple worker processes, then sort the result produced by workers and the result produced by Spark. 

Then we compare the the two sorted files, the script will output "Pass" if results are the same or "Fail" otherwise.

