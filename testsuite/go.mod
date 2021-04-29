module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.7.0
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20210429174118-60091ebbbdaf
	storj.io/storj v0.12.1-0.20210430075244-dae9464ee81b
	storj.io/uplink v1.5.0-rc.1.0.20210430080223-7a594b191100
)
