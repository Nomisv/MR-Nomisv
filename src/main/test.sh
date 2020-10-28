# tests

# cd ../


####### build apps #####

go build -buildmode=plugin wc.go || exit 1
go build -buildmode=plugin wordLength.go || exit 1
go build -buildmode=plugin occureByLength.go || exit 1

########################


####################### test single process ###################
rm -rf temp
mkdir temp || exit 1
cd temp || exit 1
rm -f temp-*

sort ../correct_sparkwordcount.txt > temp-correct.txt
# run master
go run ../master_main.go ../hamlet.txt 1 & sleep 1
# run 1 worker
go run ../worker_main.go ../wc.so

sort temp-output* > temp-wordcountAll

# test word count
echo '----------' wordcount test single-process'-----------'

if cmp temp-wordcountAll temp-correct.txt
then
    echo '----------' PASS'-----------'
else
    echo '----------' FAIL '-----------'
    exit 1
fi

# remove temp file for testing single process
rm -f temp-*
################################################################



####################### test multiple processes ####################


# ------------------ word count ---------------
sort ../correct_sparkwordcount.txt > temp-correct.txt
# run master
go run ../master_main.go ../hamlet.txt 5 & sleep 1
# run 5 workers
go run ../worker_main.go ../wc.so &
go run ../worker_main.go ../wc.so &
go run ../worker_main.go ../wc.so &
go run ../worker_main.go ../wc.so &
go run ../worker_main.go ../wc.so 

sort temp-output* > temp-wordcountAll

# test word count
echo '----------' wordcount test multi-processes'-----------'

if cmp temp-wordcountAll temp-correct.txt
then
    echo '----------' PASS'-----------'
else
    echo '----------' FAIL '-----------'
    exit 1
fi

# remove temp file for word count multi-processes
rm -f temp-*
# --------------------------------------------


# ------------------ word length --------------
sort ../correct_sparkwordLength.txt > temp-correct.txt
# run master
go run ../master_main.go ../hamlet.txt 6 & sleep 1
# run 6 workers
go run ../worker_main.go ../wordLength.so &
go run ../worker_main.go ../wordLength.so &
go run ../worker_main.go ../wordLength.so &
go run ../worker_main.go ../wordLength.so &
go run ../worker_main.go ../wordLength.so &
go run ../worker_main.go ../wordLength.so 

sort temp-output* > temp-wordcountAll

# test word count
echo '----------' word length test multi-processes'-----------'

if cmp temp-wordcountAll temp-correct.txt
then
    echo '----------' PASS'-----------'
else
    echo '----------' FAIL '-----------'
    exit 1
fi

# remove temp file for word count multi-processes
rm -f temp-*
# --------------------------------------------


# ------------------ occur by length --------------
sort ../correct_spark_occurByLength.txt > temp-correct.txt
# run master
go run ../master_main.go ../hamlet.txt 3 & sleep 1
# run 6 workers
go run ../worker_main.go ../occureByLength.so &
go run ../worker_main.go ../occureByLength.so &
go run ../worker_main.go ../occureByLength.so 

sort temp-output* > temp-wordcountAll

# test word count
echo '----------' occur by length test multi-processes'-----------'

if cmp temp-wordcountAll temp-correct.txt
then
    echo '----------' PASS'-----------'
else
    echo '----------' FAIL '-----------'
    exit 1
fi

# remove temp file for word count multi-processes
rm -f temp-*
# --------------------------------------------


################################################################



####################### test fault tolerance ####################
rm -f temp-*




#################################################################