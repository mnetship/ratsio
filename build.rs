fn main() {
    prost_build::compile_protos(&["src/protocol.proto"],
                                &["src/"]).unwrap();
}