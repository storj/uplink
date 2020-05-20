module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.5.1
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.28.0 // indirect
	storj.io/common v0.0.0-20200519171747-3ff8acf78c46
	storj.io/storj v0.12.1-0.20200520114506-2c9afe7f1762
	storj.io/uplink v1.0.6-0.20200519154233-897d83c0f564
)
