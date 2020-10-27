use std::fs::{OpenOptions, File};
use std::path::PathBuf;

pub struct TableConfig {
    pub base_folder: PathBuf,
}

impl TableConfig {
    pub fn new_file(&self, name_base: &str, extension: &str, writeable: bool) -> std::io::Result<File> {
        let mut path = self.base_folder.clone();
        path.push(format!("{}.{}", name_base, extension));

        OpenOptions::new()
            .create(writeable)
            .write(writeable)
            .read(true)
            .open(&path)
    }
}
