package main

import (
	"fmt"
)

func readFromDb(key string) string{
	return "Db entry for key " + key
}

func createCacheNode(keyCh <-chan string) <- chan string{
	c := make(chan string)
	go func(){
		for{
			cache := make(map[string]string)
			key := <- keyCh
			res, ok := cache[key]
			if(!ok){
				res = readFromDb(key)
				cache[key] = key
			}
			c <- res
		}
	}()
	return c
}

func main() {
	r := make(chan string)
	c := createCacheNode(r)
	r <- "Hello my cache"
	fmt.Println(<- c)
}
