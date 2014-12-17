package main

import (
	"encoding/gob"
	"flag"
	"fmt"
	"net"
)

type Request struct {
	Key string
}

func readFromDb(key string) string {
	return "Db entry for key " + key
}

func createCacheNode(keyCh <-chan string) <-chan string {
	fmt.Println("Cache node started")
	c := make(chan string)
	go func() {
		for {
			cache := make(map[string]string)
			key := <-keyCh
			res, ok := cache[key]
			if !ok {
				res = readFromDb(key)
				cache[key] = key
			}
			c <- res
		}
	}()
	return c
}

func createListener(port string) {
	go func() {
		addr, _ := net.ResolveUDPAddr("udp", port)
		sock, _ := net.ListenUDP("udp", addr)
		fmt.Printf("network server started at udp%s\n", port)
		for {
			dec := gob.NewDecoder(sock)
			var key string
			dec.Decode(&key)
			fmt.Printf("request for %s\n", key)
		}
	}()
}

var ports = []string{":8000", ":8001", ":8002", ":8003"}

func getValue(key string) string {
	// ask all instances
	for _, port := range ports {
		go func(port string) {
			fmt.Printf("Trying to reach %s\n",port)
			conn, err := net.Dial("udp", port)
			if err != nil {
				fmt.Printf("Couldn't connect to %s\n")
				fmt.Println(err)
				return
			}

			defer conn.Close()

			enc := gob.NewEncoder(conn)

			enc.Encode(key)
		}(port)
	}
	return key
}

func main() {
	// parse CL
	instanceId := flag.Int("id", 0, "instance id")

	flag.Parse()

	// init all
	r := make(chan string)
	createCacheNode(r)
	createListener(ports[*instanceId])

	// main loop
	var input string
	for {
		fmt.Scanln(&input)
		getValue(input)
	}
}
