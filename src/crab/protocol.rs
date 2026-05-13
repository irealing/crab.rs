#[repr(u32)]
pub enum Method {
    PING = 0,
    ACK = 1,
}
pub struct Command {
    method: Method,
}
