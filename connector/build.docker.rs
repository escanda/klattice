fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().build_server(false).compile(
        &[
            "proto/exec.proto",
            "proto/plan.proto",
            "proto/query.proto",
            "proto/messages.proto",
            "proto/oracle.proto",
        ],
        &["proto/"],
    )?;
    Ok(())
}
