use super::childinfo::*;

// TODO use proc macro
impl From<StopError> for tonic::Status {
    fn from(err: StopError) -> Self {
        tonic::Status::internal(err.to_string())
    }
}
impl From<AddClientError> for tonic::Status {
    fn from(err: AddClientError) -> Self {
        tonic::Status::internal(err.to_string())
    }
}
impl From<StatusError> for tonic::Status {
    fn from(err: StatusError) -> Self {
        tonic::Status::internal(err.to_string())
    }
}
impl From<ChildInfoCreationError> for tonic::Status {
    fn from(err: ChildInfoCreationError) -> Self {
        tonic::Status::internal(err.to_string())
    }
}
impl From<ChildInfoCreationWithCGroupError> for tonic::Status {
    fn from(err: ChildInfoCreationWithCGroupError) -> Self {
        tonic::Status::internal(err.to_string())
    }
}
