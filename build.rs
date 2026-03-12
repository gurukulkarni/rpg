use std::process::Command;

fn main() {
    // Embed git commit hash into the binary at compile time.
    let hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map_or_else(|| "unknown".to_owned(), |s| s.trim().to_owned());

    println!("cargo:rustc-env=SAMO_GIT_HASH={hash}");

    // Re-run if HEAD changes (new commit).
    println!("cargo:rerun-if-changed=.git/HEAD");
}
