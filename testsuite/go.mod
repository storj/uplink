module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.5.1
	github.com/vivint/infectious v0.0.0-20190108171102-2455b059135b
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.28.0 // indirect
	storj.io/common v0.0.0-20200529121635-ef4a5bc8ec88
	storj.io/storj v0.12.1-0.20200529133420-d6c90b7ab583
	storj.io/uplink v1.0.6
)
