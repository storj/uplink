module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.7.0
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	golang.org/x/tools/gopls v0.7.3 // indirect
	storj.io/common v0.0.0-20210708125041-4882a3ae3eda
	storj.io/storj v0.12.1-0.20210708145037-a767aed5919f
	storj.io/uplink v1.5.0-rc.1.0.20210627094806-13dc15ec9c19
)
