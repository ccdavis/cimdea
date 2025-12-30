//! SSH remote execution with ControlMaster connection pooling.
//!
//! This module provides SSH connectivity to IPUMS servers using the system `ssh`
//! command with ControlMaster multiplexing for connection reuse.
//!
//! # Connection Reuse
//!
//! When connecting to multiple environments on the same server (e.g., internal and
//! demo both on `ipums-internal-web.pop.umn.edu`), the connection is reused via
//! SSH ControlMaster sockets. Connections are automatically closed when the pool
//! is dropped.
//!
//! # Third-Party Servers
//!
//! For third-party servers (DHS, MICS), the pool can prompt the user interactively
//! for confirmation and optional custom username.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use tempfile::TempDir;

/// Error type for remote operations
#[derive(Debug)]
pub enum RemoteError {
    /// SSH connection failed
    ConnectionFailed(String),
    /// Connection was skipped (user declined third-party)
    ConnectionSkipped,
    /// Command execution failed
    CommandFailed(String),
    /// IO error
    IoError(std::io::Error),
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteError::ConnectionFailed(msg) => write!(f, "SSH connection failed: {}", msg),
            RemoteError::ConnectionSkipped => write!(f, "Connection skipped"),
            RemoteError::CommandFailed(msg) => write!(f, "Remote command failed: {}", msg),
            RemoteError::IoError(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for RemoteError {}

impl From<std::io::Error> for RemoteError {
    fn from(err: std::io::Error) -> Self {
        RemoteError::IoError(err)
    }
}

/// State of a server connection
#[derive(Debug, Clone)]
pub enum ConnectionState {
    /// Not yet attempted
    Pending,
    /// Successfully connected
    Connected {
        /// The SSH target used (may include username)
        ssh_target: String,
    },
    /// Connection failed
    Failed,
    /// User skipped (for third-party servers)
    Skipped,
}

/// Manages SSH connections with ControlMaster socket pooling
pub struct SshConnectionPool {
    /// Temporary directory for control sockets
    control_dir: TempDir,

    /// Connection state by canonical hostname
    connections: HashMap<String, ConnectionState>,

    /// Map of domain -> canonical hostname (for live servers)
    canonical_hosts: HashMap<String, String>,

    /// SSH connect timeout in seconds
    connect_timeout: u32,

    /// ControlPersist timeout in seconds
    persist_timeout: u32,
}

impl SshConnectionPool {
    /// Create a new SSH connection pool
    pub fn new() -> Result<Self, RemoteError> {
        let control_dir = TempDir::new()?;

        Ok(Self {
            control_dir,
            connections: HashMap::new(),
            canonical_hosts: HashMap::new(),
            connect_timeout: 30,
            persist_timeout: 600,
        })
    }

    /// Get the ControlPath for a given SSH target
    fn control_path(&self, target: &str) -> PathBuf {
        // Create a simple filename from the target
        let safe_name = target.replace(['@', ':'], "_");
        self.control_dir.path().join(format!("ssh-{}", safe_name))
    }

    /// Resolve a hostname to its canonical form (for connection reuse)
    fn resolve_canonical_host(&mut self, hostname: &str) -> String {
        if let Some(cached) = self.canonical_hosts.get(hostname) {
            return cached.clone();
        }

        // Use getent to resolve
        let output = Command::new("getent")
            .args(["ahosts", hostname])
            .output();

        let canonical = match output {
            Ok(out) if out.status.success() => {
                String::from_utf8_lossy(&out.stdout)
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(2))
                    .map(String::from)
                    .unwrap_or_else(|| hostname.to_string())
            }
            _ => hostname.to_string(),
        };

        self.canonical_hosts
            .insert(hostname.to_string(), canonical.clone());
        canonical
    }

    /// Get the SSH target for a connected server
    fn get_ssh_target(&self, server: &str) -> Option<String> {
        let canonical = self
            .canonical_hosts
            .get(server)
            .cloned()
            .unwrap_or_else(|| server.to_string());

        match self.connections.get(&canonical) {
            Some(ConnectionState::Connected { ssh_target }) => Some(ssh_target.clone()),
            _ => None,
        }
    }

    /// Establish an SSH connection to a server
    ///
    /// For third-party servers when `interactive` is true, this will prompt the
    /// user for confirmation and optional custom username.
    ///
    /// Returns Ok(()) if connected, Err if failed or skipped.
    pub fn connect(
        &mut self,
        server: &str,
        is_third_party: bool,
        interactive: bool,
    ) -> Result<(), RemoteError> {
        let canonical = self.resolve_canonical_host(server);

        // Check if already connected or failed
        if let Some(state) = self.connections.get(&canonical) {
            return match state {
                ConnectionState::Connected { .. } => Ok(()),
                ConnectionState::Failed => Err(RemoteError::ConnectionFailed(
                    "Previous connection attempt failed".to_string(),
                )),
                ConnectionState::Skipped => Err(RemoteError::ConnectionSkipped),
                ConnectionState::Pending => unreachable!(),
            };
        }

        // For third-party servers, prompt user
        let ssh_target = if is_third_party && interactive {
            if !self.prompt_third_party_connection(server)? {
                self.connections
                    .insert(canonical.clone(), ConnectionState::Skipped);
                return Err(RemoteError::ConnectionSkipped);
            }

            // Optionally prompt for username
            if let Some(user) = self.prompt_username(server)? {
                format!("{}@{}", user, canonical)
            } else {
                canonical.clone()
            }
        } else {
            canonical.clone()
        };

        let control_path = self.control_path(&ssh_target);

        // Establish ControlMaster connection
        let status = Command::new("ssh")
            .args([
                "-o",
                "ControlMaster=yes",
                "-o",
                &format!("ControlPath={}", control_path.display()),
                "-o",
                &format!("ControlPersist={}", self.persist_timeout),
                "-o",
                &format!("ConnectTimeout={}", self.connect_timeout),
                "-o",
                "BatchMode=no",
                "-o",
                "NumberOfPasswordPrompts=1",
                &ssh_target,
                "echo",
                "Connection successful",
            ])
            .stdin(Stdio::inherit()) // Allow password prompt
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .status()?;

        if status.success() {
            self.connections.insert(
                canonical.clone(),
                ConnectionState::Connected {
                    ssh_target: ssh_target.clone(),
                },
            );
            // Also store mapping for original server name if different
            if server != canonical {
                self.connections.insert(
                    server.to_string(),
                    ConnectionState::Connected { ssh_target },
                );
            }
            Ok(())
        } else {
            self.connections.insert(canonical, ConnectionState::Failed);
            Err(RemoteError::ConnectionFailed(format!(
                "SSH to {} failed",
                server
            )))
        }
    }

    /// Execute a command on a connected server
    pub fn exec(&self, server: &str, command: &str) -> Result<String, RemoteError> {
        let ssh_target = self
            .get_ssh_target(server)
            .ok_or_else(|| RemoteError::ConnectionFailed("Not connected".to_string()))?;

        let control_path = self.control_path(&ssh_target);

        let output = Command::new("ssh")
            .args([
                "-o",
                &format!("ControlPath={}", control_path.display()),
                "-o",
                &format!("ConnectTimeout={}", self.connect_timeout),
                &ssh_target,
                command,
            ])
            .output()?;

        if output.status.success() {
            Ok(String::from_utf8_lossy(&output.stdout).to_string())
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            Err(RemoteError::CommandFailed(format!(
                "Command failed: {}",
                stderr.trim()
            )))
        }
    }

    /// Check if a path exists on the remote server (directory or file)
    pub fn path_exists(&self, server: &str, path: &str) -> Result<bool, RemoteError> {
        let cmd = format!("test -e '{}' && echo 'yes' || echo 'no'", path);
        let output = self.exec(server, &cmd)?;
        Ok(output.trim() == "yes")
    }

    /// Check if a directory exists on the remote server
    pub fn dir_exists(&self, server: &str, path: &str) -> Result<bool, RemoteError> {
        let cmd = format!("test -d '{}' && echo 'yes' || echo 'no'", path);
        let output = self.exec(server, &cmd)?;
        Ok(output.trim() == "yes")
    }

    /// List files matching a pattern
    pub fn list_files(&self, server: &str, pattern: &str) -> Result<Vec<String>, RemoteError> {
        let cmd = format!("ls -1 {} 2>/dev/null || true", pattern);
        let output = self.exec(server, &cmd)?;
        Ok(output
            .lines()
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect())
    }

    /// Get file modification timestamps (epoch seconds)
    pub fn get_timestamps(&self, server: &str, pattern: &str) -> Result<Vec<i64>, RemoteError> {
        let cmd = format!("stat -c '%Y' {} 2>/dev/null || true", pattern);
        let output = self.exec(server, &cmd)?;
        Ok(output
            .lines()
            .filter_map(|s| s.trim().parse::<i64>().ok())
            .collect())
    }

    /// List directories that contain content (for parquet/derived)
    ///
    /// For parquet directories, this checks for .parquet files.
    /// For derived directories, this checks for any content.
    pub fn list_content_dirs(&self, server: &str, base_dir: &str) -> Result<Vec<String>, RemoteError> {
        // Check each subdirectory for parquet files or any content
        let cmd = format!(
            r#"for d in '{}'/*/ ; do
                if [ -d "$d" ]; then
                    if ls "$d"*.parquet >/dev/null 2>&1 || [ -n "$(ls -A "$d" 2>/dev/null)" ]; then
                        basename "$d"
                    fi
                fi
            done 2>/dev/null || true"#,
            base_dir
        );
        let output = self.exec(server, &cmd)?;
        Ok(output
            .lines()
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect())
    }

    /// Check if connected to a server
    pub fn is_connected(&self, server: &str) -> bool {
        let canonical = self
            .canonical_hosts
            .get(server)
            .cloned()
            .unwrap_or_else(|| server.to_string());
        matches!(
            self.connections.get(&canonical),
            Some(ConnectionState::Connected { .. })
        )
    }

    /// Get the connection state for a server
    pub fn connection_state(&self, server: &str) -> Option<&ConnectionState> {
        let canonical = self
            .canonical_hosts
            .get(server)
            .cloned()
            .unwrap_or_else(|| server.to_string());
        self.connections.get(&canonical)
    }

    // Private helper methods for interactive prompts
    fn prompt_third_party_connection(&self, server: &str) -> Result<bool, RemoteError> {
        print!(
            "{} is a third-party server. Try to connect? [y/N] ",
            server
        );
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        Ok(input.trim().eq_ignore_ascii_case("y"))
    }

    fn prompt_username(&self, server: &str) -> Result<Option<String>, RemoteError> {
        let default_user = std::env::var("USER").unwrap_or_default();
        print!("Username for {} [{}]: ", server, default_user);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let input = input.trim();
        if input.is_empty() {
            Ok(None) // Use default
        } else {
            Ok(Some(input.to_string()))
        }
    }

    /// Close all connections (called automatically on drop)
    fn close_all_connections(&mut self) {
        for (_, state) in &self.connections {
            if let ConnectionState::Connected { ssh_target } = state {
                let control_path = self.control_path(ssh_target);
                let _ = Command::new("ssh")
                    .args([
                        "-O",
                        "exit",
                        "-o",
                        &format!("ControlPath={}", control_path.display()),
                        ssh_target,
                    ])
                    .output();
            }
        }
    }
}

impl Drop for SshConnectionPool {
    fn drop(&mut self) {
        self.close_all_connections();
        // TempDir cleanup happens automatically
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_creation() {
        let pool = SshConnectionPool::new();
        assert!(pool.is_ok());
    }

    #[test]
    fn test_control_path_generation() {
        let pool = SshConnectionPool::new().unwrap();
        let path = pool.control_path("user@example.com");
        assert!(path.to_string_lossy().contains("ssh-user_example.com"));
    }

    #[test]
    fn test_not_connected_initially() {
        let pool = SshConnectionPool::new().unwrap();
        assert!(!pool.is_connected("example.com"));
    }
}
