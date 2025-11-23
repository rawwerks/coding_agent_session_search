fn main() {
    use vergen::{BuildBuilder, CargoBuilder, Emitter};

    let mut emitter = Emitter::default();

    if let Ok(build) = BuildBuilder::all_build() {
        let _ = emitter.add_instructions(&build);
    }
    if let Ok(cargo) = CargoBuilder::all_cargo() {
        let _ = emitter.add_instructions(&cargo);
    }

    if let Err(e) = emitter.emit() {
        eprintln!("vergen emit skipped: {e}");
    }
}
