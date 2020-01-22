#[allow(clippy::module_inception)]
pub mod protocol;
pub use self::protocol::*;

#[allow(bare_trait_objects)]

pub mod parser;
pub use self::parser::*;

#[cfg(test)]
mod test;