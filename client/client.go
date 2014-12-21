package client

import (
	"container/list"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	dto "github.com/arukim/overmind/data"
	"net"
	"os"
	"time"
)

// Information about request (Task)
type Request struct {
	Id        int
	Key       string
	TimeLimit time.Time
	Return    chan interface{}
	LocOwner  *DataOwnerInfo
	Start     time.Time
	NodeCount int
}

//Information about node, which owns requested data
type DataOwnerInfo struct {
	Id           int
	Addr         string
	HasValue     bool
	ItemsInCache int
}

// Host Command server (UDP)
func runCommandServer(queueRun chan DataOwnerInfo) {
	addr, _ := net.ResolveUDPAddr("udp", _conf.HostAddr)
	sock, _ := net.ListenUDP("udp", addr)

	fmt.Printf("network server started at udp %s\n", _conf.HostAddr)
	dec := json.NewDecoder(sock)
	for {
		req := dto.DiscoverResponse{}
		dec.Decode(&req)
		owner := DataOwnerInfo{
			Id:           req.Id,
			Addr:         req.Addr,
			HasValue:     req.HasValue,
			ItemsInCache: req.Elements}
		queueRun <- owner
	}
}

// Connect to client and download data
func downloadData(request Request, readFlag bool) {
	// connect via tcp
	conn, err := net.Dial("tcp", request.LocOwner.Addr)
	if err != nil {
		fmt.Printf("cannot conect to data owner %v", request.LocOwner.Addr)
	}
	defer conn.Close()
	// send data request
	encoder := gob.NewEncoder(conn)
	req := dto.DataReqPacket{Key: request.Key, ReadFlag: readFlag}
	encoder.Encode(req)
	// receive data
	decoder := gob.NewDecoder(conn)
	resp := dto.DataRespPacket{}
	decoder.Decode(&resp)
	request.Return <- resp.Value
}

// Proccess one packet command response packet from server
func checkOneOwner(tasks *list.List, owner *DataOwnerInfo) {
	for e := tasks.Front(); e != nil; e = e.Next() {
		req := e.Value.(*Request)
		if req.Id == owner.Id {
			req.NodeCount--
			if owner.HasValue {
				tasks.Remove(e)
				req.LocOwner = owner
				go downloadData(*req, false)
			} else {
				if req.LocOwner == nil {
					req.LocOwner = owner
				} else {
					if req.LocOwner.ItemsInCache > owner.ItemsInCache {
						req.LocOwner = owner
					}
				}
				if req.NodeCount == 0 {
					if req.LocOwner != nil {
						go downloadData(*req, true)
					}
					tasks.Remove(e)
					return
				}
			}
		}
	}
}

// Check if some task have expired (possibly due to offline server)
func checkForTaskExpiration(tasks *list.List) {
	for e := tasks.Front(); e != nil; e = e.Next() {
		req := e.Value.(*Request)
		if time.Since(req.Start) > time.Duration(_conf.CheckLimit)*time.Millisecond {
			if req.LocOwner != nil {
				go downloadData(*req, true)
			}
			tasks.Remove(e)
		}
	}
}

// Any request for cache is stored in queque, until it's is fullfield
// Incoming packets are muxed to tasks
// return params
// queueAdd chan Request - add new Request to Queue
// queueRun chan DataOwnerInfo - Input for server packets
func createRequestQueue() (chan Request, chan DataOwnerInfo) {
	queueAdd := make(chan Request, len(_conf.Nodes))
	queueRun := make(chan DataOwnerInfo)

	go func() {
		tasks := list.New()
		for {
			select {
			case req := <-queueAdd: // Add
				tasks.PushBack(&req)

			case owner := <-queueRun: //Start download process, remove request from queue
				checkOneOwner(tasks, &owner)
			case <-time.After(time.Duration(_conf.CheckLimit/2) * time.Millisecond):
				checkForTaskExpiration(tasks)
			}
		}
	}()
	return queueAdd, queueRun
}

func Get(key string, result *interface{}, executionLimit, expire time.Duration, getter func()) error {
	_currId++
	resultCh := make(chan interface{}, 1)
	timeLimit := time.Now().Add(-expire)
	req := Request{
		Id:        _currId,
		Key:       key,
		TimeLimit: timeLimit,
		Return:    resultCh,
		NodeCount: len(_conf.Nodes),
		Start:     time.Now(),
	}
	_queueAdd <- req
	// broadcast all nodes same packet
	packet := dto.DiscoverRequest{Key: key, Addr: _conf.HostAddr, Id: _currId, TimeLimit: timeLimit}
	for _, port := range _conf.Nodes {
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
			enc.Encode(packet)
		}(port)
	}

	select {
	case *result = <-resultCh:
		getter()
		return nil
	case <-time.After(executionLimit): // timeout
		return errors.New("Execution limit reached")
	}
}

type Configuration struct {
	Nodes      []string
	CheckLimit int
	HostAddr   string
}

var _currId int // TODO: it's NOT thread safe
var _queueAdd chan Request
var _queueRun chan DataOwnerInfo
var _conf Configuration

func init() {
	file, _ := os.Open("om_conf.json")
	defer file.Close()
	decoder := json.NewDecoder(file)
	err := decoder.Decode(&_conf)
	if err != nil {
		panic(err)
	}

	_queueAdd, _queueRun = createRequestQueue()
	go runCommandServer(_queueRun)
}
