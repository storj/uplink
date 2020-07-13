module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.5.1
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	go.uber.org/zap v1.15.0
	golang.org/x/sync v0.0.0-20200317015054-43a5402ce75a
	google.golang.org/grpc v1.28.0 // indirect
	storj.io/common v0.0.0-20200701134427-63fe7147a3f3
	storj.io/storj v0.12.1-0.20200710204345-24a1eac16c59
	storj.io/uplink v1.1.3-0.20200629150500-6274a2c623d6
)
