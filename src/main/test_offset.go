package main

import (
	"fmt"
	"io"
	"os"
)

func appendToFile(fileName string) error {

	f, err := os.Open(fileName)
	if err != nil {
		fmt.Println("failed open file" + err.Error())
	} else {
		// 查找文件末尾的偏移量
		n, _ := f.Seek(0, 0)
		println(n)
		// 从末尾的偏移量开始写入内容
		bs := make([]byte, 100)
		// content, err := f.ReadAt(bs, n)

		a := ""
		i := 0
		for true {
			bs = make([]byte, 100+i)

			_, err := f.ReadAt(bs, n)
			//END OF FILE
			if err == io.EOF {
				break
			}
			//end of line
			a = string(bs)
			if a[len(a)-1:] == "\n" {
				break
			}
			i++
		}

		println(string(bs))
	}
	defer f.Close()
	return err
}

func main() {

	appendToFile("hamlet2.txt")

}
