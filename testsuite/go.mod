module storj.io/uplink/testsuite

go 1.20

replace storj.io/uplink => ../

require (
	github.com/klauspost/compress v1.17.7
	github.com/stretchr/testify v1.9.0
	github.com/zeebo/errs v1.3.0
	go.uber.org/zap v1.27.0
	golang.org/x/exp v0.0.0-20231006140011-7918f672742d
	golang.org/x/sync v0.6.0
	storj.io/common v0.0.0-20240611143318-a6f1908a63e9
	storj.io/drpc v0.0.34
	storj.io/infectious v0.0.2
	storj.io/storj v1.91.0-alpha.0.20240405184421-d601509eca53
	storj.io/uplink v1.12.3-0.20240405162132-4ad5621e6841
)

require (
	github.com/alicebob/gopher-json v0.0.0-20200520072559-a9ecdc9d1d3a // indirect
	github.com/alicebob/miniredis/v2 v2.13.3 // indirect
	github.com/apache/thrift v0.17.0 // indirect
	github.com/blang/semver v3.5.1+incompatible // indirect
	github.com/bmkessler/fastdiv v0.0.0-20190227075523-41d5178f2044 // indirect
	github.com/boombuler/barcode v1.0.1-0.20190219062509-6c824513bacc // indirect
	github.com/calebcase/tmpfile v1.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cloudfoundry/gosigar v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/flynn/noise v1.0.0 // indirect
	github.com/fsnotify/fsnotify v1.7.0 // indirect
	github.com/go-oauth2/oauth2/v4 v4.4.2 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt v3.2.1+incompatible // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/pprof v0.0.0-20230602150820-91b7bce49751 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/gorilla/schema v1.2.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgerrcode v0.0.0-20201024163028-a0d42d470451 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgtype v1.14.1 // indirect
	github.com/jackc/pgx/v5 v5.5.2 // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/jtolds/monkit-hw/v2 v2.0.0-20191108235325-141a0da276b3 // indirect
	github.com/jtolds/tracetagger/v2 v2.0.0-rc5 // indirect
	github.com/jtolio/crawlspace v0.0.0-20231116162947-3ec5cc6b36c5 // indirect
	github.com/jtolio/crawlspace/tools v0.0.0-20231116162947-3ec5cc6b36c5 // indirect
	github.com/jtolio/mito v0.0.0-20230523171229-d78ef06bb77b // indirect
	github.com/jtolio/noiseconn v0.0.0-20230301220541-88105e6c8ac6 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-sqlite3 v1.14.19 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/onsi/ginkgo/v2 v2.9.5 // indirect
	github.com/oschwald/maxminddb-golang v1.12.0 // indirect
	github.com/pelletier/go-toml/v2 v2.1.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/pquerna/otp v1.3.0 // indirect
	github.com/quic-go/qtls-go1-20 v0.4.1 // indirect
	github.com/quic-go/quic-go v0.40.1 // indirect
	github.com/redis/go-redis/v9 v9.0.3 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/sagikazarmark/locafero v0.4.0 // indirect
	github.com/sagikazarmark/slog-shim v0.1.0 // indirect
	github.com/segmentio/backo-go v0.0.0-20200129164019-23eae7c10bd3 // indirect
	github.com/shopspring/decimal v1.2.0 // indirect
	github.com/sourcegraph/conc v0.3.0 // indirect
	github.com/spacemonkeygo/monkit/v3 v3.0.22 // indirect
	github.com/spacemonkeygo/spacelog v0.0.0-20180420211403-2296661a0572 // indirect
	github.com/spacemonkeygo/tlshowdy v0.0.0-20160207005338-8fa2cec1d7cd // indirect
	github.com/spf13/afero v1.11.0 // indirect
	github.com/spf13/cast v1.6.0 // indirect
	github.com/spf13/cobra v1.8.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/spf13/viper v1.18.2 // indirect
	github.com/stripe/stripe-go/v75 v75.8.0 // indirect
	github.com/subosito/gotenv v1.6.0 // indirect
	github.com/xtgo/uuid v0.0.0-20140804021211-a0b114877d4c // indirect
	github.com/yuin/gopher-lua v0.0.0-20191220021717-ab39c6098bdb // indirect
	github.com/zeebo/admission/v3 v3.0.3 // indirect
	github.com/zeebo/blake3 v0.2.3 // indirect
	github.com/zeebo/errs/v2 v2.0.5 // indirect
	github.com/zeebo/float16 v0.1.0 // indirect
	github.com/zeebo/goof v0.0.0-20230907150950-e9457bc94477 // indirect
	github.com/zeebo/incenc v0.0.0-20180505221441-0d92902eec54 // indirect
	github.com/zeebo/mwc v0.0.4 // indirect
	github.com/zeebo/structs v1.0.3-0.20230601144555-f2db46069602 // indirect
	github.com/zeebo/sudo v1.0.2 // indirect
	github.com/zyedidia/generic v1.2.1 // indirect
	go.etcd.io/bbolt v1.3.5 // indirect
	go.uber.org/mock v0.3.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.19.0 // indirect
	golang.org/x/mod v0.13.0 // indirect
	golang.org/x/net v0.21.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/time v0.5.0 // indirect
	golang.org/x/tools v0.14.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/segmentio/analytics-go.v3 v3.1.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	storj.io/eventkit v0.0.0-20240415002644-1d9596fee086 // indirect
	storj.io/monkit-jaeger v0.0.0-20240221095020-52b0792fa6cd // indirect
	storj.io/picobuf v0.0.3 // indirect
)
