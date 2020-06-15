module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.5.1
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.28.0 // indirect
	storj.io/common v0.0.0-20200611114417-9a3d012fdb62
	storj.io/storj v0.12.1-0.20200616113141-6673125c035f
	storj.io/uplink v1.1.1
)
