fn main() {
    tonic_build::configure()
        .compile(&["grpc/runkins.proto"], &["grpc"]) //"../proto"
        .unwrap();
}
