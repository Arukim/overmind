package data

import(
	"time"
)


// DTO, mixed for req/resp in UDP
type DiscoverRequest struct {
	Id       int    // Command Id
	Key      string // Requested key
	Addr     string // Response point
	TimeLimit time.Time
}

type DiscoverResponse struct {
	Id int
	Addr string
	HasValue bool
}

// DTO, TCP request for data
type DataReqPacket struct {
	Key      string
	ReadFlag bool
	TimeLimit time.Time
}

// DTO, TCP response with requested data
type DataRespPacket struct {
	Value string // Data
}

