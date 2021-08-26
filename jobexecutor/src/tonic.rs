use super::childinfo::*;

impl From<ChildInfoError> for tonic::Status {
    fn from(err: ChildInfoError) -> Self {
        tonic::Status::internal(err.to_string())
    }
}
