pub mod file_handler;
pub mod migration;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    BadMigration,
    MigrationDirNotFound,
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
