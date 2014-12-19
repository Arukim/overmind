package main

import (
	"container/list"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"time"
)

// DTO, command type
const (
	VAL_REQ  = 1 + iota // Request about value presence in cache
	VAL_RESP            // Response with info about value
)

// DTO, mixed for req/resp in UDP
type ComPacket struct {
	Type     int    // Command type
	Id       int    // Command Id
	Key      string // Requested key
	Port     string // Response point
	HasValue bool
}

// DTO, TCP request for data
type DataReqPacket struct {
	Key      string
	ReadFlag bool
}

// DTO, TCP response with requested data
type DataRespPacket struct {
	Value string // Data
}

// Information about request for data
type Request struct {
	Id       int
	Key      string
	Callback chan DataOwnerInfo
	Cancel   chan int
	Exit     chan bool
}

//Information about node, which owns requested data
type DataOwnerInfo struct {
	Id           int
	Addr         string
	HasValue     bool
	ItemsInCache int
}

// read data from database
func readFromDb(key string) string {
	return "Db entry for key " + key
}

// Request to cache
type CacheRequest struct {
	Key        string
	ReadFromDb bool
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
	req := DataReqPacket{}
	dec.Decode(&req)

	cacheReq := CacheRequest{Key: req.Key, ReadFromDb: req.ReadFlag}
	fmt.Println("hDataConnection: Requesting db")
	cacheReqCh <- cacheReq
	resp := DataRespPacket{Value: <-cacheResp}
	fmt.Println("hDataConnection: sending response")
	enc := gob.NewEncoder(conn)
	enc.Encode(resp)
}

// Data server is simply handling incoming data connections
func runDataServer(port string, cacheReq chan CacheRequest, cacheResp <-chan string) {
	sock, _ := net.Listen("tcp", port)
	for {
		conn, err := sock.Accept()
		if err != nil {
			continue
		}
		fmt.Println("New connection")
		go handleDataConnection(conn, cacheReq, cacheResp)
	}
}

// Here command packets are parsed and executed
// If ValueRequest packet is received - local cache is checked for this value
// and if it's present ValueResponse packet is returned
// For ValueResponse packet - signal is sent to TaskQueue using quequeRun chan
func runCommHandler(in <-chan ComPacket, port string, cacheReqCh chan CacheRequest,
	cacheResp <-chan string, queueRun chan DataOwnerInfo) {
	for {
		packet := <-in
		switch packet.Type {
		case VAL_REQ:
			conn, err := net.Dial("udp", packet.Port)
			if err != nil {
				fmt.Printf("Couldn't connect to %s\n", packet.Port)
				fmt.Println(err)
				return
			}
			defer conn.Close()

			cacheReq := CacheRequest{Key: packet.Key, ReadFromDb: false}
			cacheReqCh <- cacheReq
			val := <-cacheResp
			var resp ComPacket
			if val != "" {
				resp.HasValue = true
			} else {
				resp.HasValue = false
			}
			resp.Type = VAL_RESP
			resp.Port = port
			resp.Id = packet.Id

			enc := json.NewEncoder(conn)
			enc.Encode(resp)
		case VAL_RESP:
			fmt.Printf("response is %v\n", packet)
			owner := DataOwnerInfo{Id: packet.Id,
				Addr:     packet.Port,
				HasValue: packet.HasValue}
			queueRun <- owner

		}
	}
}

// Host Command server (UDP)
func runCommandServer(port string, cacheReq chan CacheRequest, cacheResp <-chan string,
	queueRun chan DataOwnerInfo) {
	handlerCh := make(chan ComPacket)
	go runCommHandler(handlerCh, port, cacheReq, cacheResp, queueRun)
	addr, _ := net.ResolveUDPAddr("udp", port)
	sock, _ := net.ListenUDP("udp", addr)
	fmt.Printf("network server started at udp%s\n", port)
	dec := json.NewDecoder(sock)
	for {
		req := ComPacket{}
		dec.Decode(&req)
		handlerCh <- req
	}
}

func downloadData(request Request, owner DataOwnerInfo, readFlag bool) {
	// connect via tcp
	conn, err := net.Dial("tcp", owner.Addr)
	if err != nil {
		fmt.Println("cannot conect to data owner")
	}
	defer conn.Close()
	// send data request
	encoder := gob.NewEncoder(conn)
	req := DataReqPacket{Key: request.Key, ReadFlag: readFlag}
	encoder.Encode(req)

	// receive data
	decoder := gob.NewDecoder(conn)
	resp := DataRespPacket{}
	decoder.Decode(&resp)
	fmt.Printf("data received %s\n", resp.Value)
}

// Wait until data is found in cache or request data from db
func waitForValue(req Request) {
	var locOwner *DataOwnerInfo // less occupied owner
PACKET_LOOP:
	for {
		select {
		case owner := <-req.Callback: // data owner found
			if owner.HasValue {
				req.Cancel <- req.Id
				go downloadData(req, owner, false)
				break PACKET_LOOP
			} else {
				// find less occupied owner
				if locOwner == nil {
					locOwner = &owner
				} else {
					if locOwner.ItemsInCache > owner.ItemsInCache {
						locOwner = &owner
					}
				}
			}
		case <-time.After(time.Millisecond * 30): // timeout
			fmt.Println("timeout!")

			req.Cancel <- req.Id
			if locOwner != nil {
				// no one has value, so ask less occupied one to get it from db
				go downloadData(req, *locOwner, true)
			}
			break PACKET_LOOP
		}
	}
	// deadlock protection
	for {
		select {
		case <-req.Callback:
		case <-req.Exit:
			return
		}
	}
}

// Any request for cache is stored in queque, until it's is fullfield
// return params
// queueAdd chan Request - add new Request to Queue
// queueRun chan DataOwnerInfo - Signals that data owner is found
// queueRemove - removes item from queue (error or timeout)
func createRequestQueue() (chan Request, chan DataOwnerInfo, chan int) {
	queueAdd := make(chan Request)
	queueRun := make(chan DataOwnerInfo)
	queueRemove := make(chan int)

	go func() {
		tasks := list.New()
		for {
			select {
			case req := <-queueAdd: // Add
				//fmt.Printf("Addind %v to queue\n",req)
				tasks.PushBack(req)

			case owner := <-queueRun: //Start download process, remove request from queue
				//fmt.Printf("Running %v owner\n", owner)
				for e := tasks.Front(); e != nil; e = e.Next() {
					if e.Value.(Request).Id == owner.Id {
						e.Value.(Request).Callback <- owner
					}
				}
			case id := <-queueRemove: //Remove item from queue
				for e := tasks.Front(); e != nil; e = e.Next() {
					if e.Value.(Request).Id == id {
						e.Value.(Request).Exit <- true
						tasks.Remove(e)
					}
				}
			}
		}
	}()
	return queueAdd, queueRun, queueRemove
}

var currId int

func getValue(key, hostPort string, quequeAdd chan Request, cancel chan int) {
	currId++
	var callback = make(chan DataOwnerInfo)
	var exit = make(chan bool)
	req := Request{Id: currId, Key: key, Callback: callback, Cancel: cancel, Exit: exit}
	go waitForValue(req)
	quequeAdd <- req
	// broadcast all instances
	for _, port := range ports {
		go func(port string) {
			conn, err := net.Dial("udp", port)
			if err != nil {
				fmt.Printf("getValue couldn't connect to %s\n", port)
				fmt.Println(err)
				return
			}
			defer conn.Close()

			// send request packet
			enc := json.NewEncoder(conn)
			packet := ComPacket{Type: VAL_REQ, Key: key, Port: hostPort, Id: currId}
			enc.Encode(packet)
		}(port)
	}
}

var ports = []string{
	"127.0.0.1:8000",
	"127.0.0.1:8001",
	"127.0.0.1:8002",
	"127.0.0.1:8003",
}

func main() {
	// parse CL
	instanceId := flag.Int("id", 0, "instance id")

	flag.Parse()
	port := ports[*instanceId]

	// init all
	cacheReq, cacheResp := createCacheNode()
	queueAdd, queueRun, queueRemove := createRequestQueue()
	go runCommandServer(port, cacheReq, cacheResp, queueRun)
	go runDataServer(port, cacheReq, cacheResp)

	getValue("a", port, queueAdd, queueRemove)
	// main loop
	var input string
	for {
		fmt.Scanln(&input)
		getValue(input, ports[*instanceId], queueAdd, queueRemove)
	}
}
