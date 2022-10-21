build_release:
	cargo build --release

install: build_release
	install target/release/kueue_client ~/.local/bin/kueue
	install target/release/kueue_server ~/.local/bin/kueue_server
	install target/release/kueue_worker ~/.local/bin/kueue_worker
	install target/release/kueue_start_workers ~/.local/bin/kueue_start_workers

clean:
	cargo clean
