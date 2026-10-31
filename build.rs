use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
    println!("cargo:rerun-if-env-changed=BTFIND_GIT_REVISION");
    println!("cargo:rerun-if-changed=build.rs");
    for name in [
        Some("HEAD".to_string()),
        Some("packed-refs".to_string()),
        git(&["symbolic-ref", "-q", "HEAD"]),
    ]
    .into_iter()
    .flatten()
    {
        if let Some(path) = git(&["rev-parse", "--git-path", &name]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let revision = std::env::var("BTFIND_GIT_REVISION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| git(&["rev-parse", "HEAD"]));
    if let Some(revision) = revision {
        println!("cargo:rustc-env=BTFIND_GIT_REVISION={revision}");
    } else {
        println!("cargo:warning=Git revision unavailable; set BTFIND_GIT_REVISION when building a source archive for production");
    }
}
