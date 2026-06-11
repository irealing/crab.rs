use std::{
    fs::{File, read_to_string},
    io::BufReader,
    sync::Arc,
};

use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
};
use serde::Deserialize;

use crate::crab::CrabError;

#[derive(Deserialize, Debug)]
pub struct Config {
    use_system_ca: bool,
    ca_path: Option<String>,
    priv_key: String,
    cert: String,
    verify_client: bool,
}

impl Config {
    const DEFAULT_PRIV_KEY: &str = "private.key";
    const DEFAULT_CERT: &str = "cert.pem";
    const DEFAULT_CONFIG_FILENAME: &str = "config.toml";

    pub fn load_config_file(filename: &str) -> Self {
        match read_to_string(filename) {
            Ok(content) => toml::from_str(&content).unwrap_or_else(|err| {
                log::warn!("deserialize config file {} error {}", filename, err);
                Self::default()
            }),
            Err(err) => {
                log::warn!("read config file {} failed {}", filename, err);
                Self::default()
            }
        }
    }
    pub fn load_default_config_file() -> Self {
        Self::load_config_file(Self::DEFAULT_CONFIG_FILENAME)
    }
}
impl Default for Config {
    fn default() -> Self {
        Self {
            use_system_ca: true,
            ca_path: None,
            priv_key: Self::DEFAULT_PRIV_KEY.to_string(),
            cert: Self::DEFAULT_CERT.to_string(),
            verify_client: false,
        }
    }
}
pub struct TLSProvider {
    cfg: Config,
}

impl TLSProvider {
    pub fn from_config(cfg: Config) -> Self {
        Self { cfg }
    }
    pub fn from_default_config_file() -> Self {
        Self::from_config(Config::load_default_config_file())
    }
    fn load_root_cert(&self) -> Result<RootCertStore, CrabError> {
        let mut root_ca = RootCertStore::empty();
        if self.cfg.use_system_ca {
            rustls_native_certs::load_native_certs()
                .certs
                .into_iter()
                .try_for_each(|cert| root_ca.add(cert))?;
        }
        if let Some(ca_path) = &self.cfg.ca_path {
            let file = File::open(&ca_path)?;
            let mut reader = BufReader::new(file);
            rustls_pemfile::certs(&mut reader).try_for_each(|cert| -> Result<(), CrabError> {
                let cert = cert?;
                root_ca.add(cert)?;
                Ok(())
            })?;
        }
        Ok(root_ca)
    }
    pub fn build_server_config(&self) -> Result<ServerConfig, CrabError> {
        let builder = ServerConfig::builder();
        let builder_with_auth = if self.cfg.verify_client {
            let root_ca = self.load_root_cert()?;
            let client_auth = WebPkiClientVerifier::builder(Arc::new(root_ca)).build()?;
            builder.with_client_cert_verifier(client_auth)
        } else {
            builder.with_no_client_auth()
        };
        Ok(builder_with_auth.with_single_cert(self.load_cert()?, self.load_priv_key()?)?)
    }
    fn load_priv_key(&self) -> Result<PrivateKeyDer<'static>, CrabError> {
        let file = File::open(&self.cfg.priv_key)?;
        let mut reader = BufReader::new(file);
        let key = rustls_pemfile::private_key(&mut reader)?
            .ok_or(CrabError::ErrorCode(CrabError::KEY_NOT_FOUND))?;
        Ok(key)
    }
    fn load_cert(&self) -> Result<Vec<CertificateDer<'static>>, CrabError> {
        let file = File::open(&self.cfg.cert)?;
        let mut reader = BufReader::new(file);
        let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
        Ok(certs)
    }
    pub fn build_client_config(&self) -> Result<ClientConfig, CrabError> {
        let builder = ClientConfig::builder().with_root_certificates(self.load_root_cert()?);
        if self.cfg.verify_client {
            Ok(builder.with_client_auth_cert(self.load_cert()?, self.load_priv_key()?)?)
        } else {
            Ok(builder.with_no_client_auth())
        }
    }
}
#[cfg(test)]
mod tests {
    use super::TLSProvider;

    #[test]
    fn load_root_cert() {
        TLSProvider::from_default_config_file()
            .load_root_cert()
            .expect("load root cert failed");
    }
    #[test]
    fn build_server_config() {
        TLSProvider::from_default_config_file()
            .build_server_config()
            .expect("build server config failed");
    }
    #[test]
    fn build_client_config() {
        TLSProvider::from_default_config_file()
            .build_client_config()
            .expect("build client config failed");
    }
}
