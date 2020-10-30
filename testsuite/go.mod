module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.6.1
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20200324143707-d3edc9973b7e // indirect
	storj.io/common v0.0.0-20201030140758-31112c1cc750
	storj.io/storj v0.12.1-0.20201030153108-fd8e697ab240
	storj.io/uplink v1.3.2-0.20201028181609-f6efc8fcf771
)
