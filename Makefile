.PHONY: bump-dependencies
bump-dependencies:
	go get storj.io/common@main
	go mod tidy
	cd testsuite;\
		go get storj.io/common@main storj.io/storj@main storj.io/uplink@main;\
		cp go.mod go-multipart.mod;\
		go get --modfile go-multipart.mod storj.io/common@main storj.io/storj@multipart-upload storj.io/uplink@main;\
		go mod tidy