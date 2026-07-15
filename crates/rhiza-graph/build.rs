use std::{env, path::Path};

fn main() {
    println!("cargo:rerun-if-env-changed=OPENSSL_LIB_DIR");
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return;
    }

    let configured = env::var("OPENSSL_LIB_DIR").ok();
    let library_dir = configured.as_deref().or_else(|| {
        [
            "/opt/homebrew/opt/openssl@3/lib",
            "/usr/local/opt/openssl@3/lib",
        ]
        .into_iter()
        .find(|candidate| Path::new(candidate).is_dir())
    });
    if let Some(library_dir) = library_dir {
        println!("cargo:rustc-link-search=native={library_dir}");
    }
}
