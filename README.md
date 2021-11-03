# Libuplink

Go library for Storj V3 Network.

[![Go Report Card](https://goreportcard.com/badge/storj.io/uplink)](https://goreportcard.com/report/storj.io/uplink)
[![Go Doc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](https://pkg.go.dev/storj.io/uplink)

<img src="https://github.com/storj/storj/raw/main/resources/logo.png" width="100">

Storj is building a decentralized cloud storage network.
[Check out our white paper for more info!](https://storj.io/whitepaper)

----

Storj is an S3-compatible platform and suite of decentralized applications that
allows you to store data in a secure and decentralized manner. Your files are
encrypted, broken into little pieces and stored in a global decentralized
network of computers. Luckily, we also support allowing you (and only you) to
retrieve those files!

[![Introducing Storj DCS—Decentralized Cloud Storage for Developers](https://img.youtube.com/vi/JgKdBRIyIps/hqdefault.jpg)](https://www.youtube.com/watch?v=JgKdBRIyIps)

### Installation

```
go get storj.io/uplink
```

### Example

Ready to use example can be found here: [examples/walkthrough/main.go](examples/walkthrough/main.go)

Provided example requires Access Grant as an input parameter. Access Grant can be obtained from Satellite UI. [See our documentation](https://docs.storj.io/dcs/getting-started/quickstart-uplink-cli/uploading-your-first-object/create-first-access-grant).

### A Note about Versioning

Our versioning in this repo is intended to primarily support the expectations of the
[Go modules](https://blog.golang.org/using-go-modules) system, so you can expect that
within a major version release, backwards-incompatible changes will be avoided at high
cost.

# Documentation

- [Go Doc](https://pkg.go.dev/storj.io/uplink)
- [Libuplink Walkthrough](https://github.com/storj/storj/wiki/Libuplink-Walkthrough)

# Language bindings

- [Uplink-C](https://github.com/storj/uplink-c)

# License

This library is distributed under the
[MIT license](https://opensource.org/licenses/MIT) (also known as the Expat license).

# Support

If you have any questions or suggestions please reach out to us on [our community forum](https://forum.storj.io/) or file a support ticket at https://support.storj.io.
