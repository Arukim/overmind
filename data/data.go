package data



// DTO, mixed for req/resp in UDP
type DiscoverRequest struct {
	Id       int    // Command Id
	Key      string // Requested key
	Addr     string // Response point}
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
}

// DTO, TCP response with requested data
type DataRespPacket struct {
	Value string // Data
}

