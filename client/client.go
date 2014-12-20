package client

import (
	"encoding/gob"
	"encoding/json"
	dto "github.com/arukim/overmind/data"
	"fmt"
	"net"
	"time"
	"container/list"
	"errors"
)

// Information about request for data
type Request struct {
	Id       int
	Key      string
	Callback chan DataOwnerInfo
	TimeLimit time.Time
	Return   chan interface{}
}

//Information about node, which owns requested data
type DataOwnerInfo struct {
	Id           int
	Addr         string
	HasValue     bool
	ItemsInCache int
}

func runComHandler(packet *dto.DiscoverResponse, queueRun chan DataOwnerInfo){
	fmt.Printf("response is %v\n", packet)
	owner := DataOwnerInfo{Id: packet.Id,
		Addr:     packet.Addr,
		HasValue: packet.HasValue}
	queueRun <- owner
}

// Host Command server (UDP)
func runCommandServer(port string, queueRun chan DataOwnerInfo) {
	addr, _ := net.ResolveUDPAddr("udp", port)
	sock, _ := net.ListenUDP("udp", addr)
	fmt.Printf("network server started at udp%s\n", port)
	dec := json.NewDecoder(sock)
	for {
		req := dto.DiscoverResponse{}
		dec.Decode(&req)
		go runComHandler(&req, queueRun)
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
	req := dto.DataReqPacket{Key: request.Key, ReadFlag: readFlag}
	encoder.Encode(req)

	// receive data
	decoder := gob.NewDecoder(conn)
	resp := dto.DataRespPacket{}
	decoder.Decode(&resp)
	request.Return <- resp.Value
	fmt.Printf("data received %s\n", resp.Value)
}


// Wait until data is found in cache or request data from db
func waitForValue(req Request) {
	var locOwner *DataOwnerInfo // less occupied owner
	for {
		select {
		case owner := <-req.Callback: // data owner found
			if owner.HasValue {
				_queueRemove <- req.Id
				go downloadData(req, owner, false)
				return
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

			_queueRemove <- req.Id
			if locOwner != nil {
				// no one has value, so ask less occupied one to get it from db
				go downloadData(req, *locOwner, true)
			}
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
	queueAdd := make(chan Request, len(ports))
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
						tasks.Remove(e)
					}
				}
			}
		}
	}()
	return queueAdd, queueRun, queueRemove
}


var ports = []string{
	"127.0.0.1:8000",
	"127.0.0.1:8001",
	"127.0.0.1:8002",
	"127.0.0.1:8003",
}

var _currId int
var _queueAdd chan Request
var _queueRemove chan int
var _queueRun chan DataOwnerInfo
var _hostPort string

func Get(key string, result *interface{}, executionLimit, expire time.Duration, getter func())(error){
	_currId++
	callback := make(chan DataOwnerInfo)
	resultCh := make(chan interface{}, 1)
	timeLimit := time.Now().Add(-expire)
	req := Request{
		Id: _currId,
		Key: key,
		Callback: callback,
		TimeLimit: timeLimit,
		Return: resultCh,
	}
	go waitForValue(req)
	_queueAdd <- req
	// broadcast all instances
	packet := dto.DiscoverRequest{Key: key, Addr: _hostPort, Id: _currId, TimeLimit: timeLimit}
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
			enc.Encode(packet)
		}(port)
	}

	select{
	case *result = <- resultCh:
		getter()
		return nil
	case <-time.After(executionLimit): // timeout
		return errors.New("Execution limit reached")
	}
}

func init(){
	_hostPort = "localhost:8666"
	_queueAdd, _queueRun, _queueRemove = createRequestQueue();
	go runCommandServer(_hostPort, _queueRun)
}
