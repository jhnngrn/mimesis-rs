//! Identity parsing and validation.
//!
//! Identities are `user@domain` strings, lowercased.

use std::fmt;

#[derive(Debug, Clone)]
pub struct IdentityError(pub String);

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for IdentityError {}

/// Parse a `user@domain` identity into (user, domain).
pub fn parse_identity(id: &str) -> Result<(String, String), IdentityError> {
    let id = id.to_lowercase();
    let at = id
        .find('@')
        .filter(|&i| i > 0 && i < id.len() - 1)
        .ok_or_else(|| IdentityError(format!("invalid identity {:?} (want user@domain)", id)))?;
    let user = &id[..at];
    let domain = &id[at + 1..];
    if !valid_username(user) {
        return Err(IdentityError(format!("invalid username {:?}", user)));
    }
    if !valid_domain(domain) {
        return Err(IdentityError(format!("invalid domain {:?}", domain)));
    }
    Ok((user.to_string(), domain.to_string()))
}

/// True if u is a valid username (a-z, 0-9, _, -, .).
pub fn valid_username(u: &str) -> bool {
    !u.is_empty()
        && u.len() <= 64
        && u.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}

/// True if d is a valid domain (host or host:port).
pub fn valid_domain(d: &str) -> bool {
    if d.is_empty() {
        return false;
    }
    let (host, port_str) = match d.rfind(':') {
        Some(i) => (&d[..i], Some(&d[i + 1..])),
        None => (d, None),
    };
    if let Some(port) = port_str {
        if port.is_empty() || port.len() > 5 || !port.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }
    }
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    host.chars()
        .all(|c| c == '.' || c == '-' || c.is_ascii_lowercase() || c.is_ascii_digit())
}

/// Split an already-validated identity into (user, domain).
pub fn split_identity(id: &str) -> (&str, &str) {
    match id.find('@') {
        Some(i) => (&id[..i], &id[i + 1..]),
        None => (id, ""),
    }
}

/// Extract the domain part of a peer identity.
pub fn peer_domain(peer: &str) -> &str {
    split_identity(peer).1
}

/// True if the identity belongs to the given domain.
pub fn identity_in_domain(identity: &str, domain: &str) -> bool {
    split_identity(identity).1 == domain
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_identity() {
        let (user, domain) = parse_identity("Alice@Example.COM").unwrap();
        assert_eq!(user, "alice");
        assert_eq!(domain, "example.com");
    }

    #[test]
    fn parse_identity_with_port() {
        let (user, domain) = parse_identity("bob@localhost:8471").unwrap();
        assert_eq!(user, "bob");
        assert_eq!(domain, "localhost:8471");
    }

    #[test]
    fn parse_invalid_identity() {
        assert!(parse_identity("noatsign").is_err());
        assert!(parse_identity("@domain").is_err());
        assert!(parse_identity("user@").is_err());
        assert!(parse_identity("").is_err());
    }

    #[test]
    fn split_identity_works() {
        assert_eq!(split_identity("alice@example.com"), ("alice", "example.com"));
        assert_eq!(peer_domain("bob@localhost:8471"), "localhost:8471");
    }

    #[test]
    fn identity_domain_check() {
        assert!(identity_in_domain("alice@example.com", "example.com"));
        assert!(!identity_in_domain("alice@other.com", "example.com"));
    }

    #[test]
    fn valid_usernames() {
        assert!(valid_username("alice"));
        assert!(valid_username("bob.jones"));
        assert!(valid_username("user-1_2"));
        assert!(!valid_username(""));
        assert!(!valid_username("has space"));
        assert!(!valid_username("has@at"));
    }

    #[test]
    fn valid_domains() {
        assert!(valid_domain("example.com"));
        assert!(valid_domain("localhost:8471"));
        assert!(!valid_domain(""));
        assert!(!valid_domain(":8471"));
    }
}
