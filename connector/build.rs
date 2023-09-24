fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().build_server(false).compile(
        &[
            "../core/src/main/proto/exec.proto",
            "../core/src/main/proto/plan.proto",
            "../core/src/main/proto/query.proto",
            "../core/src/main/proto/messages.proto",
            "../core/src/main/proto/oracle.proto",
        ],
        &["../core/src/main/proto/", "../proto/"],
    )?;
    Ok(())
}
