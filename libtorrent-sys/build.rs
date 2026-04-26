use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=wrapper.h");
    println!("cargo:rerun-if-changed=src/cpp/libtorrent_ffi.cpp");
    
    // Compile C++ code
    let mut build = cc::Build::new();
    build.cpp(true)
          .file("src/cpp/libtorrent_ffi.cpp")
          .include("/usr/include")
          .flag("-std=c++14")
          .flag("-fPIC");
    
    // Try to find libtorrent includes
    if let Ok(include_path) = env::var("LIBTORRENT_INCLUDE_PATH") {
        build.include(include_path);
    }
    
    build.compile("libtorrent_ffi");
    
    // Link with libtorrent-rasterbar
    println!("cargo:rustc-link-lib=torrent-rasterbar");
    println!("cargo:rustc-link-lib=stdc++");
    
    // Generate Rust bindings
    let bindings = bindgen::Builder::default()
        .header("wrapper.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}