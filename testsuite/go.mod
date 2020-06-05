module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.5.1
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.28.0 // indirect
	storj.io/common v0.0.0-20200601223809-7af8b7ee5d6c
	storj.io/storj v0.12.1-0.20200605163905-943eb872d31b
	storj.io/uplink v1.0.7
)
