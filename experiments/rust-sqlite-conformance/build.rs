fn main() {
    println!(
        "cargo:rustc-env=CONFORMANCE_TARGET={}",
        std::env::var("TARGET").expect("Cargo provides TARGET")
    );
}
