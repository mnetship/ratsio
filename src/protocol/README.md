## Protocol parsers for NATS and NATS Streaming.

```
cargo install protobuf-codegen   #To get code generator for Rust
```
```
go get github.com/gogo/protobuf  #To get proto files that rust proto file depends on.
```

or remove go bits from the proto file
```
protoc  -I=. -I=$GOPATH/src --rust_out . protocol.proto
```