fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/klattice/hub.proto")?;
    tonic_build::compile_protos("proto/klattice/plan.proto")?;
    tonic_build::compile_protos("proto/klattice/status.proto")?;
    Ok(())
}