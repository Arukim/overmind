package main

import (
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	dto "github.com/arukim/overmind/data"
	_ "github.com/go-sql-driver/mysql"
	"net"
	"time"
)

type CacheRequest struct {
	Key        string
	ReadFromDb bool
	TimeLimit  time.Time
}

type CacheResponse struct {
	Successed bool
	Value     interface{}
	Elements  int
}

type CacheEntry struct {
	CreationTime time.Time
	Value        interface{}
}

func readFromDb(key string) interface{} {
	db, err := sql.Open("mysql", *_connectString)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	defer db.Close()
	var data []byte
	var result interface{}
	err = db.QueryRow("select `value` from cache where `key` = ?", key).Scan(&data)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	json.Unmarshal(data, &result)

	return result
}

// Here is cache (hashmap)
func createCacheNode() (chan CacheRequest, <-chan CacheResponse) {
	fmt.Println("Cache node started")
	cacheReq := make(chan CacheRequest)
	cacheResp := make(chan CacheResponse)
	cache := make(map[string]CacheEntry)
	elements := 0
	go func() {
		for {
			resp := CacheResponse{Elements: elements} // we don't need to send exact value
			req := <-cacheReq
			if req.ReadFromDb {
				entry := CacheEntry{}
				entry.CreationTime = time.Now()
				entry.Value = readFromDb(req.Key)
				cache[req.Key] = entry
				elements++
			}
			res, ok := cache[req.Key]
			
			// Not in cache
			if !ok {
				cacheResp <- resp
				continue
			}

			// Expiration
			if res.CreationTime.Sub(req.TimeLimit) < 0 {
				delete(cache, req.Key)
				elements--

				cacheResp <- resp
				continue
			}

			//Success
			resp.Value = res.Value
			resp.Successed = true
			cacheResp <- resp
		}
	}()
	return cacheReq, cacheResp
}

// If client is connected for data, try to get it from cache and return to the client
func handleDataConnection(conn net.Conn, cacheReqCh chan CacheRequest, cacheResp <-chan CacheResponse) {
	defer conn.Close()
	dec := gob.NewDecoder(conn)
	req := dto.DataReqPacket{}
	dec.Decode(&req)

	cacheReq := CacheRequest{Key: req.Key, ReadFromDb: req.ReadFlag, TimeLimit: req.TimeLimit}
	cacheReqCh <- cacheReq
	resp := <-cacheResp
	if resp.Successed {
		resp := dto.DataRespPacket{Value: resp.Value}
		enc := gob.NewEncoder(conn)
		enc.Encode(resp)
	}
}

// Data server is simply handling incoming data connections
func runDataServer(port string, cacheReq chan CacheRequest, cacheResp <-chan CacheResponse) {
	sock, _ := net.Listen("tcp", port)
	fmt.Printf("data server is listening on tcp%s\n", port)
	for {
		conn, err := sock.Accept()
		if err != nil {
			continue
		}
		go handleDataConnection(conn, cacheReq, cacheResp)
	}
}

// Here command packets are executed (sending resp to req)
func runComHandler(packet *dto.DiscoverRequest, port string, cacheReqCh chan CacheRequest, cacheResp <-chan CacheResponse) {
	conn, err := net.Dial("udp", packet.Addr)
	if err != nil {
		fmt.Printf("Couldn't connect to %s\n", packet.Addr)
		fmt.Println(err)
		return
	}
	defer conn.Close()

	cacheReq := CacheRequest{Key: packet.Key, ReadFromDb: false, TimeLimit: packet.TimeLimit}
	cacheReqCh <- cacheReq
	val := <-cacheResp
	var resp dto.DiscoverResponse
	resp.HasValue = val.Successed
	resp.Elements = val.Elements
	resp.Addr = port
	resp.Id = packet.Id

	enc := json.NewEncoder(conn)
	enc.Encode(resp)
}

// Host Command server (UDP)
func runCommandServer(port string, cacheReq chan CacheRequest, cacheResp <-chan CacheResponse) {
	addr, _ := net.ResolveUDPAddr("udp", port)
	sock, _ := net.ListenUDP("udp", addr)
	fmt.Printf("command server is listening on udp%s\n", port)
	dec := json.NewDecoder(sock)
	for {
		req := dto.DiscoverRequest{}
		dec.Decode(&req)
		go runComHandler(&req, port, cacheReq, cacheResp)
	}
}

var _connectString *string

func main() {
	// parse CL
	instanceTcpPort := flag.String("tcp", ":8000", "tcp port")
	instanceUdpPort := flag.String("udp", ":8000", "udp port")
	_connectString = flag.String("cs", "", "connection string")

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
