use crab::proto::HandshakePacket;
use serde::{Deserialize, Serialize};
use sysinfo::{Disks, System};

#[derive(Deserialize, Serialize, Debug)]
pub struct Handshake {
    pub device_id: String,
    pub host_info: HostInfo,
}
#[derive(Deserialize, Serialize, Debug)]
pub struct HostInfo {
    pub version: String,
    pub hostname: String,
    pub os_version: String,
    pub disk_info: Vec<DiskInfo>,
}
#[derive(Deserialize, Serialize, Debug)]
pub struct DiskInfo {
    pub path: String,
    pub size: u64,
    pub used: u64,
}
impl HostInfo {
    const UNKNOWN: &str = "unknown";
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname: System::host_name().unwrap_or(Self::UNKNOWN.to_string()),
            os_version: System::long_os_version().unwrap_or(Self::UNKNOWN.to_string()),
            disk_info: Disks::new_with_refreshed_list()
                .iter()
                .map(|x| DiskInfo {
                    path: x.mount_point().to_string_lossy().to_string(),
                    size: x.total_space(),
                    used: x.available_space(),
                })
                .collect(),
        }
    }
}
impl HandshakePacket for Handshake {
    fn node_id(&self) -> &str {
        &self.device_id
    }
}
impl Handshake {
    pub fn new(device_id: &str) -> Self {
        Self {
            device_id: device_id.to_string(),
            host_info: HostInfo::new(),
        }
    }
}
#[derive(Deserialize, Serialize, Debug)]
pub enum Command {
    Ping,
}
