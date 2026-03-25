// AuthSubHandler is the sub-handler type for authentication and ACL operations.
//
// Dependencies live on the main Handler struct; this type exists only
// as a grouping marker referenced by Handler.
module kafka

/// AuthSubHandler is the sub-handler type for SASL and ACL request handling.
/// All dependencies are accessed through the parent Handler struct.
pub struct AuthSubHandler {
}
