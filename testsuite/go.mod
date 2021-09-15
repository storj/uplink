module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.7.0
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20210915201516-56ad343b6a7e
	storj.io/drpc v0.0.25
	storj.io/private v0.0.0-20210719004409-d6bcdddb82e0 // indirect
	storj.io/storj v0.12.1-0.20210909071551-c258f4bbac77
	storj.io/uplink v1.5.0-rc.1.0.20210908131054-2c4f221fe461
)
