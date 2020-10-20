# go build -buildmode=plugin wc.go &
# go run master_main.go hamlet.txt 2 &
go run worker_main.go wc.so &
go run worker_main.go wc.so &
go run worker_main.go wc.so

sort sparkResult.txt > correct.txt

sort output* > wordcountAll

# test word count
echo '----------' wordcount test'-----------'

if cmp wordcountAll correct.txt
then
    echo '----------' PASS'-----------'
else
    echo '----------' FAIL '-----------'
    exit 1
fi

