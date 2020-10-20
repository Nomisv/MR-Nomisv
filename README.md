# Milestone 2

Group: Xueteng Qian, Haiyang Wu

We have the same code and files.

In this stage, 


# Run Test

`$ cd src/main`

We firstly start master process in one terminal: (6 workers)

`Terminal 1 $ go run master_main.go hamlet.txt 6`

Then run the test in the other terminal:

`Terminal 2 $ sh test.sh`

The test script will start multiple worker processes, then sort the result produced by workers and the result produced by Spark. 

Then we compare the the two sorted files, the script will output "Pass" if results are the same or "Fail" otherwise.

