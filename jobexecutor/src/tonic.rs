macro_rules! from_err_to_tonic_status {
    ($($err_name:ty),+) => {
        $(
        impl From<$err_name> for ::tonic::Status {
            fn from(err: $err_name) -> Self {
                ::tonic::Status::internal(err.to_string())
            }
        }
        )+
    };
}

from_err_to_tonic_status!(
    super::childinfo::StopError,
    super::childinfo::AddClientError,
    super::childinfo::StatusError,
    super::childinfo::ChildInfoCreationError,
    super::childinfo::ChildInfoCreationWithCGroupError
);
