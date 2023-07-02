use std::{
    env,
    error::Error,
    fs::{self},
    path::{Path, PathBuf},
};

const PROTO_ROOT: &str = "pkg/api/proto";

/// Add Substrait version information to the build
fn klattice_version() -> Result<(), Box<dyn Error>> {
    use git2::{DescribeFormatOptions, DescribeOptions, Repository};

    let klattice_version_file =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR")?).join("src/version.in");

    // Rerun if the Substrait submodule changed (to allow setting `dirty`)
    println!(
        "cargo:rerun-if-changed={}",
        Path::new(PROTO_ROOT).display()
    );

    // Get the version from the submodule
    match Repository::open("../../") {
        Ok(repo) => {
            // Get describe output
            let mut describe_options = DescribeOptions::default();
            describe_options.describe_tags();
            let mut describe_format_options = DescribeFormatOptions::default();
            describe_format_options.always_use_long_format(true);
            describe_format_options.dirty_suffix("-dirty");
            let git_describe = repo
                .describe(&describe_options)?
                .format(Some(&describe_format_options))?;

            let mut split = git_describe.split('-');
            let git_version = split.next().unwrap_or_default();
            let git_depth = split.next().unwrap_or_default();
            let git_dirty = git_describe.ends_with("dirty");
            let git_hash = repo.head()?.peel_to_commit()?.id().to_string();
            let semver::Version {
                major,
                minor,
                patch,
                ..
            } = semver::Version::parse(git_version.trim_start_matches('v'))?;

            fs::write(
                klattice_version_file,
                format!(
                    r#"// SPDX-License-Identifier: Private license

// Note that this file is auto-generated and auto-synced using `build.rs`. It is
// included in `version.rs`.

/// The major version of Substrait used to build this crate
pub const KLATTICE_MAJOR_VERSION: u64 = {major};

/// The minor version of Substrait used to build this crate
pub const KLATTICE_MINOR_VERSION: u64 = {minor};

/// The patch version of Substrait used to build this crate
pub const KLATTICE_PATCH_VERSION: u64 = {patch};

/// The Git SHA (lower hex) of Substrait used to build this crate
pub const KLATTICE_GIT_SHA: &str = "{git_hash}";

/// The `git describe` output of the Substrait submodule used to build this
/// crate
pub const KLATTICE_GIT_DESCRIBE: &str = "{git_describe}";

/// The amount of commits between the latest tag and this version of the 
/// Substrait module used to build this crate
pub const KLATTICE_GIT_DEPTH: u32 = {git_depth};

/// The dirty state of the Substrait submodule used to build this crate
pub const KLATTICE_GIT_DIRTY: bool = {git_dirty};
"#
                ),
            )?;
        }
        Err(e) => {
            // If this is a package build the `KLATTICE_version_file` should
            // exist. If it does not, it means this is probably a Git build that
            // did not clone the substrait submodule.
            if !klattice_version_file.exists() {
                panic!("W: {e}")
            }
        }
    };

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");

    klattice_version()?;

    std::env::set_var("PROTOC", protobuf_src::protoc());

    let protos = ["proto/klattice/api.proto", "proto/klattice/plan.proto", "proto/klattice/status.proto"]
        .into_iter()
        .inspect(|entry| {
            println!("cargo:rerun-if-changed={}", entry);
        })
        .collect::<Vec<_>>();

    tonic_build::configure()
        .build_server(false)
        .compile(&protos, &["proto/"])?;

    Ok(())
}