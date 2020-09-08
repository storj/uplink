module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.5.1
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	go.uber.org/zap v1.15.0
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	google.golang.org/grpc v1.28.0 // indirect
	storj.io/common v0.0.0-20200908141706-a7deca2c1806
	storj.io/storj v0.12.1-0.20200908142027-9fd97fa97301
	storj.io/uplink v1.3.0
)
