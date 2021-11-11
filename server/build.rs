fn main() {
    tonic_build::configure()
        .compile(&["../proto/runkins/runkins.proto"], &["../proto"])
        .unwrap();
}
