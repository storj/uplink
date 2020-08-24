module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.5.1
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	go.uber.org/zap v1.15.0
	golang.org/x/sync v0.0.0-20200625203802-6e8e738ad208
	google.golang.org/grpc v1.28.0 // indirect
	storj.io/common v0.0.0-20200818131620-f9cddf66b4be
	storj.io/storj v0.12.1-0.20200826132400-4f28bf07207f
	storj.io/uplink v1.2.1-0.20200819113156-adca79cc8927
)
