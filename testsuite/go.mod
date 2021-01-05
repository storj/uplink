module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/onsi/ginkgo v1.14.0 // indirect
	github.com/stretchr/testify v1.6.1
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	golang.org/x/net v0.0.0-20200707034311-ab3426394381 // indirect
	storj.io/common v0.0.0-20201210184814-6206aefd1d48
	storj.io/storj v0.12.1-0.20210104173745-710b86849cea
	storj.io/uplink v1.4.3
)
