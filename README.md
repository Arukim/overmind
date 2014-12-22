Overmind
========

# About

Distributed cache written in Golang.

# Requirements

* MySql is used as DB, install it or change main.go->readFromDb func for your own db

* Please, "go get github.com/go-sql-driver/mysql". It's required for sql connections, or you can change
main.go to use your favorite sql driver

* Project is build and tested on Debian 7 amd64 / go1.3.2

* Please check Tools https://github.com/Arukim/overmind_tools for sample client or db initializer application

# Setup

* go install github.com/go-sql-driver/mysql

* Init DB - you can use db_init.go tool from Tools. Pass connection string using -cs "login:pass@tcp(host:port)/database e.g. "arukim:1234@tcp(127.0.0.1:3306)/overmind. This tool will create table "cache" with 10k elements with size 10..50kb.

* go install github.com/arukim/overmind/

* Run server instances, e.g. "/bin/overmind -tcp 8000 -udp 8000 -cs "arukim:1234@tcp(127.0.0.1:3306)/overmind)"

* go build sample client from Tools - test_client.go

* create config file "om_conf.json", write all server instances to Nodes. Select client host addr. Set "CheckLimit" - this value should be not less than doubled average ping between client and server.

* now run test_client.go





