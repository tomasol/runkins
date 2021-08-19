fn main() {
    tonic_build::configure()
        .compile(&["proto/jobexecutor/job_executor.proto"], &["proto"])
        .unwrap();
}
