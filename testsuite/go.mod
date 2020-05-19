module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/gomodule/redigo v2.0.0+incompatible // indirect
	github.com/sirupsen/logrus v1.4.2 // indirect
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.14.1
	google.golang.org/grpc v1.28.0 // indirect
	storj.io/common v0.0.0-20200519144636-6a729faf9037
	storj.io/storj v0.12.1-0.20200511172612-93f1fe49e3eb
	storj.io/uplink v1.0.6-0.20200511061052-a46fe83636d6
)
