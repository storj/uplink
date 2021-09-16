module storj.io/uplink/testsuite

go 1.13

replace storj.io/uplink => ../

require (
	github.com/stretchr/testify v1.7.0
	github.com/vivint/infectious v0.0.0-20200605153912-25a574ae18a3
	github.com/zeebo/errs v1.2.2
	go.uber.org/zap v1.16.0
	storj.io/common v0.0.0-20210916151047-6aaeb34bb916
	storj.io/drpc v0.0.26
	storj.io/private v0.0.0-20210719004409-d6bcdddb82e0 // indirect
	storj.io/storj v0.12.1-0.20210916114455-b2d724962c24
	storj.io/uplink v1.5.0-rc.1.0.20210915202907-4aeb0a767a86
)
