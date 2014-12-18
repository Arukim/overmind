package main

import (
	"encoding/json"
	"encoding/gob"
	"container/list"
	"flag"
	"fmt"
	"net"
	"time"
)

// DTO, command type
const (
	VAL_REQ = 1 + iota  // Request about value presence in cache
	VAL_RESP // Response with info about value
)

// DTO, mixed for req/resp in UDP
type ComPacket struct {
	Type int // Command type
	Id int // Command Id
	Key string // Requested key
	Port string // Response point
}

// DTO, TCP request for data
type DataReqPacket struct {
	Key string
}

// DTO, TCP response with requested data
type DataRespPacket struct {
	Value string // Data
}

// Information about request for data
type Request struct { 
	Id int
	Key string
	Callback chan DataOwnerInfo
	Cancel chan int
}

//Information about node, which owns requested data
type DataOwnerInfo struct { 
	Id int
	Addr string
}

// read data from database
func readFromDb(key string) string {
	return "Db entry for key " + key
}

// Here is cache (hashmap)
func createCacheNode() (chan string, <-chan string) {
	fmt.Println("Cache node started")
	cacheReq := make(chan string)
	cacheResp := make(chan string)
	go func() {
		for {
			cache := make(map[string]string)
			key := <-cacheReq
			res, ok := cache[key]
			if !ok {
				res = readFromDb(key)
				cache[key] = key
			}
			cacheResp <- res
		}
	}()
	return cacheReq, cacheResp
}

// If client is connected for data, try to get it from cache and return to the client
func handleDataConnection(conn net.Conn, cacheReq chan string, cacheResp <- chan string){
	defer conn.Close()
	dec := gob.NewDecoder(conn)
	req := DataReqPacket{}
	dec.Decode(&req)
	cacheReq <- req.Key
	resp := DataRespPacket {Value: <- cacheResp}
	enc := gob.NewEncoder(conn)
	enc.Encode(resp)	
}

// Data server is simply handling incoming data connections 
func runDataServer(port string, cacheReq chan string, cacheResp <- chan string){
	sock, _ := net.Listen("tcp",port)
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
func runCommHandler(in <-chan ComPacket, port string, cacheReq chan string,
	cacheResp <- chan string, queueRun chan DataOwnerInfo){
	for {
		packet := <-in
		switch(packet.Type){
		case VAL_REQ:
			cacheReq <- packet.Key
			<- cacheResp
			var resp ComPacket
			resp.Type = VAL_RESP
			resp.Port = port
			resp.Id = packet.Id
			conn, err := net.Dial("udp", packet.Port)
			if err != nil {
				fmt.Printf("Couldn't connect to %s\n",packet.Port)
				fmt.Println(err)
				return
			}
			defer conn.Close()
			enc := json.NewEncoder(conn)
			enc.Encode(resp)
		case VAL_RESP:
			fmt.Printf("response is %v\n", packet)
			owner := DataOwnerInfo{ Id: packet.Id, Addr: packet.Port}
			queueRun <- owner
		}
	}
}

// Host Command server (UDP)
func runCommandServer(port string, cacheReq chan string, cacheResp <- chan string,
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

// Wait until data is found on network or cancel task
func waitForValue(req Request){
	select{
	case owner := <- req.Callback: // data owner found
		// connect via tcp
		conn, err := net.Dial("tcp", owner.Addr)
		if err != nil {
			fmt.Println("cannot conect to data owner")
		}
		defer conn.Close()
		// send data request
		encoder := gob.NewEncoder(conn)
		req := DataReqPacket{Key: req.Key}
		encoder.Encode(req)

		// receive data
		decoder := gob.NewDecoder(conn)
		resp := DataRespPacket{}
		decoder.Decode(&resp)
		fmt.Printf("data received %s\n", resp.Value)		
	case <- time.After(time.Millisecond * 30):// timeout
		req.Cancel <- req.Id
	}
}

// Any request for cache is stored in queque, until it's is fullfield
// return params
// queueAdd chan Request - add new Request to Queue
// queueRun chan DataOwnerInfo - Signals that data owner is found
// queueRemove - removes item from queue (error or timeout)
func createRequestQueue()(chan Request, chan DataOwnerInfo, chan int){
	queueAdd := make(chan Request)
	queueRun := make(chan DataOwnerInfo)
	queueRemove := make(chan int)
	
	go func(){
		tasks := list.New()
		for {
			select{
			case req := <-queueAdd: // Add
//				fmt.Printf("Addind %v to queue\n",req)
				tasks.PushBack(req)
				
			case owner := <-queueRun: //Start download process, remove request from queue
//				fmt.Printf("Running %v owner\n", owner)
				for e := tasks.Front(); e !=nil; e = e.Next(){
					if e.Value.(Request).Id == owner.Id {
						e.Value.(Request).Callback <- owner
						tasks.Remove(e)
					}
				}
			case id := <- queueRemove: //Remove item from queue
				for e := tasks.Front(); e != nil; e = e.Next(){
					if e.Value.(Request).Id == id {
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
	req := Request { Id: currId, Key: key, Callback: callback, Cancel: cancel}
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
			packet := ComPacket{Type: VAL_REQ, Key: key, Port: hostPort, Id : currId}
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
