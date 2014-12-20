package main

import (
	"encoding/gob"
	"encoding/json"
	dto "github.com/arukim/overmind/data"
	"flag"
	"fmt"
	"net"
)

// Request to cache
type CacheRequest struct {
	Key        string
	ReadFromDb bool
}

// read data from database
func readFromDb(key string) string {
	return "Db entry for key " + key
}

// Here is cache (hashmap)
func createCacheNode() (chan CacheRequest, <-chan string) {
	fmt.Println("Cache node started")
	cacheReq := make(chan CacheRequest)
	cacheResp := make(chan string)
	cache := make(map[string]string)
	go func() {
		for {
			req := <-cacheReq
			fmt.Println("request received")
			if req.ReadFromDb {
				fmt.Println("read key from db")
				cache[req.Key] = readFromDb(req.Key)
			}
			res, ok := cache[req.Key]
			if !ok {
				cacheResp <- ""
			} else {
				fmt.Println("returning key")
				cacheResp <- res
			}
		}
	}()
	return cacheReq, cacheResp
}

// If client is connected for data, try to get it from cache and return to the client
func handleDataConnection(conn net.Conn, cacheReqCh chan CacheRequest, cacheResp <-chan string) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)
	req := dto.DataReqPacket{}
	dec.Decode(&req)

	cacheReq := CacheRequest{Key: req.Key, ReadFromDb: req.ReadFlag}
	fmt.Println("hDataConnection: Requesting db")
	cacheReqCh <- cacheReq
	resp := dto.DataRespPacket{Value: <-cacheResp}
	fmt.Println("hDataConnection: sending response")
	enc := gob.NewEncoder(conn)
	enc.Encode(resp)
}

// Data server is simply handling incoming data connections
func runDataServer(port string, cacheReq chan CacheRequest, cacheResp <-chan string) {
	sock, _ := net.Listen("tcp", port)
	for {
		fmt.Printf("data server is listening on tcp%s\n",port)
		conn, err := sock.Accept()
		if err != nil {
			continue
		}
		go handleDataConnection(conn, cacheReq, cacheResp)
	}
}

// Here command packets are parsed and executed
// If ValueRequest packet is received - local cache is checked for this value
// and if it's present ValueResponse packet is returned
// For ValueResponse packet - signal is sent to TaskQueue using quequeRun chan
func runComHandler(in <-chan dto.DiscoverRequest, port string, cacheReqCh chan CacheRequest,
	cacheResp <-chan string) {
	for {
		packet := <-in
		go func(dto.DiscoverRequest){
			conn, err := net.Dial("udp", packet.Addr)
			if err != nil {
				fmt.Printf("Couldn't connect to %s\n", packet.Addr)
				fmt.Println(err)
				return
			}
			defer conn.Close()
			
			cacheReq := CacheRequest{Key: packet.Key, ReadFromDb: false}
			cacheReqCh <- cacheReq
			val := <-cacheResp
			var resp dto.DiscoverResponse
			if val != "" {
				resp.HasValue = true
			} else {
				resp.HasValue = false
			}
			resp.Addr = port
			resp.Id = packet.Id
			
			enc := json.NewEncoder(conn)
			enc.Encode(resp)

		}(packet)	
	}
}

// Host Command server (UDP)
func runCommandServer(port string, cacheReq chan CacheRequest, cacheResp <-chan string) {
	handlerCh := make(chan dto.DiscoverRequest)
	go runComHandler(handlerCh, port, cacheReq, cacheResp)
	addr, _ := net.ResolveUDPAddr("udp", port)
	sock, _ := net.ListenUDP("udp", addr)
	fmt.Printf("command server is listening on udp%s\n", port)
	dec := json.NewDecoder(sock)
	for {
		req := dto.DiscoverRequest{}
		dec.Decode(&req)
		handlerCh <- req
	}
}


func main() {
	// parse CL
	instanceTcpPort := flag.String("tcp",":8000", "tcp port")
	instanceUdpPort := flag.String("udp",":8000", "udp port")

	flag.Parse()

	// init all
	cacheReq, cacheResp := createCacheNode()
	go runCommandServer(*instanceUdpPort, cacheReq, cacheResp)
	go runDataServer(*instanceTcpPort, cacheReq, cacheResp)

	// main loop
	var input string
	fmt.Println("Press any key to exit")
	fmt.Scanln(&input)
}
