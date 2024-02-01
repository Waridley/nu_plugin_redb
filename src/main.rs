use nu_plugin::{serve_plugin, MsgPackSerializer};
use nu_plugin_redb::RedbPlugin;

fn main() {
	serve_plugin(&mut RedbPlugin {}, MsgPackSerializer {})
}
