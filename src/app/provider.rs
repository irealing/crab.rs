use super::Manager;

struct ServiceProvider {
    manager: Manager,
}
impl ServiceProvider {
    fn manager(&self) -> Manager {
        self.manager.clone()
    }
}
