mod crab;
use crab::utils::crypto::Config;

use crate::crab::utils::crypto::TLSProvider;
fn main() {
    let cfg = Config::load_default_config_file();
    println!("local config {:?}", &cfg);
    TLSProvider::from_config(cfg).build_client_config().unwrap();
}
