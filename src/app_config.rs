//! Environment configuration for the `ask` tool.
//!
//! A checked-in TOML file (`cimdea.toml`) names, for each environment (e.g. `dev` and `prod`), the
//! file holding that environment's Gemini API key and the IPUMS data root to tabulate against. The
//! environments model the dev/prod split: `prod` points at the organization's Cloud Console Gemini
//! project and its deployed data; `dev` points at an individual developer's own Gemini account and
//! their local data. The API key *files* are not checked in (see `.gitignore`); each
//! developer/deployment supplies its own.
//!
//! Paths in the config support a leading `~` (home directory) and `$VAR` / `${VAR}` environment
//! variables; relative paths resolve against the config file's own directory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::mderror::MdError;

/// The default config file name looked up in the current directory.
pub const DEFAULT_CONFIG_FILE: &str = "cimdea.toml";

/// The parsed `cimdea.toml`.
#[derive(Debug, Deserialize)]
pub struct AppConfig {
    /// Which environment to use when none is named on the command line.
    #[serde(default)]
    pub default_environment: Option<String>,
    /// Named environments (e.g. "dev", "prod").
    #[serde(default)]
    pub environments: BTreeMap<String, EnvConfig>,
    /// Directory the config file lives in; relative paths resolve against it. Set by [Self::load],
    /// not read from the TOML.
    #[serde(skip)]
    base_dir: PathBuf,
}

/// One environment's settings.
#[derive(Debug, Deserialize)]
pub struct EnvConfig {
    /// Path to the file holding this environment's Gemini API key (not checked in).
    pub api_key_file: String,
    /// IPUMS data root for this environment (the directory containing `parquet/` and `layouts/`).
    pub data_root: String,
}

/// An environment resolved to concrete paths, ready to use.
#[derive(Debug, Clone)]
pub struct ResolvedEnvironment {
    /// The environment name that was selected.
    pub name: String,
    /// Absolute path to the API key file.
    pub api_key_path: PathBuf,
    /// Resolved IPUMS data root.
    pub data_root: String,
}

impl ResolvedEnvironment {
    /// Read and trim the API key from its file. Errors if the file is missing or empty.
    pub fn read_api_key(&self) -> Result<String, MdError> {
        let key = std::fs::read_to_string(&self.api_key_path)
            .map_err(|e| {
                MdError::Msg(format!(
                    "could not read the API key file {} for environment '{}': {e}",
                    self.api_key_path.display(),
                    self.name
                ))
            })?
            .trim()
            .to_string();
        if key.is_empty() {
            return Err(MdError::Msg(format!(
                "the API key file {} for environment '{}' is empty",
                self.api_key_path.display(),
                self.name
            )));
        }
        Ok(key)
    }
}

impl AppConfig {
    /// Load and parse a config file. The file's parent directory becomes the base for resolving
    /// relative paths.
    pub fn load(path: &Path) -> Result<Self, MdError> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            MdError::Msg(format!(
                "could not read config file {}: {e}",
                path.display()
            ))
        })?;
        let mut cfg: AppConfig = toml::from_str(&text).map_err(|e| {
            MdError::Msg(format!(
                "could not parse config file {}: {e}",
                path.display()
            ))
        })?;
        cfg.base_dir = path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        Ok(cfg)
    }

    /// Find the default config file (`cimdea.toml`) in the current directory, if it exists.
    pub fn find_default() -> Option<PathBuf> {
        let path = PathBuf::from(DEFAULT_CONFIG_FILE);
        path.is_file().then_some(path)
    }

    /// Resolve the requested environment (or the configured default) into concrete paths.
    pub fn resolve(&self, requested: Option<&str>) -> Result<ResolvedEnvironment, MdError> {
        let name = requested
            .map(str::to_string)
            .or_else(|| self.default_environment.clone())
            .ok_or_else(|| {
                MdError::Msg(
                    "no environment was requested and the config has no default_environment"
                        .to_string(),
                )
            })?;

        let env = self.environments.get(&name).ok_or_else(|| {
            let mut available: Vec<&str> = self.environments.keys().map(String::as_str).collect();
            available.sort_unstable();
            MdError::Msg(format!(
                "environment '{name}' is not defined in the config; available: {}",
                available.join(", ")
            ))
        })?;

        Ok(ResolvedEnvironment {
            name,
            api_key_path: resolve_path(&env.api_key_file, &self.base_dir),
            data_root: resolve_path(&env.data_root, &self.base_dir)
                .to_string_lossy()
                .into_owned(),
        })
    }
}

/// Expand a leading `~` and any `$VAR` / `${VAR}`, then resolve a relative result against `base`.
fn resolve_path(raw: &str, base: &Path) -> PathBuf {
    let expanded = expand(raw);
    let path = PathBuf::from(&expanded);
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

fn home_dir() -> Option<String> {
    std::env::var("HOME").ok().filter(|s| !s.is_empty())
}

/// Expand a leading `~`/`~/` to `$HOME` and replace `$VAR` / `${VAR}` with environment values.
fn expand(raw: &str) -> String {
    let tilde_expanded = if raw == "~" {
        home_dir().unwrap_or_else(|| raw.to_string())
    } else if let Some(rest) = raw.strip_prefix("~/") {
        match home_dir() {
            Some(home) => format!("{home}/{rest}"),
            None => raw.to_string(),
        }
    } else {
        raw.to_string()
    };
    expand_env_vars(&tilde_expanded)
}

/// Replace `$VAR` and `${VAR}` occurrences with environment variable values (empty if unset).
fn expand_env_vars(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.char_indices().peekable();
    while let Some((idx, c)) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        let rest = &s[idx + 1..];
        if let Some(after_brace) = rest.strip_prefix('{') {
            if let Some(end) = after_brace.find('}') {
                let var = &after_brace[..end];
                out.push_str(&std::env::var(var).unwrap_or_default());
                for _ in 0..(end + 2) {
                    chars.next(); // consume '{', the name, and '}'
                }
                continue;
            }
        } else {
            let name_len = rest
                .chars()
                .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
                .count();
            if name_len > 0 {
                let var: String = rest.chars().take(name_len).collect();
                out.push_str(&std::env::var(&var).unwrap_or_default());
                for _ in 0..name_len {
                    chars.next();
                }
                continue;
            }
        }
        out.push(c); // a lone '$' with no valid variable name
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_path_absolute_and_relative() {
        let base = Path::new("/cfg/dir");
        assert_eq!(resolve_path("/abs/path", base), PathBuf::from("/abs/path"));
        assert_eq!(resolve_path("rel/x", base), PathBuf::from("/cfg/dir/rel/x"));
    }

    #[test]
    fn test_expand_tilde_and_home() {
        if let Some(home) = home_dir() {
            assert_eq!(expand("~/ipums_usa_data"), format!("{home}/ipums_usa_data"));
            assert_eq!(expand("$HOME/data"), format!("{home}/data"));
            assert_eq!(expand("${HOME}/data"), format!("{home}/data"));
        }
    }

    #[test]
    fn test_expand_unset_var_is_empty() {
        assert_eq!(expand("$DEFINITELY_UNSET_VAR_XYZ/tail"), "/tail");
    }

    #[test]
    fn test_load_and_resolve() {
        // Write a config + key file into a temporary directory and resolve "prod".
        let dir = std::env::temp_dir().join(format!("cimdea_cfg_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("prod_key.txt"), "  SECRET-KEY\n").unwrap();
        let toml = r#"
            default_environment = "dev"

            [environments.dev]
            api_key_file = "dev_key.txt"
            data_root = "~/dev_data"

            [environments.prod]
            api_key_file = "prod_key.txt"
            data_root = "/srv/ipums"
        "#;
        let cfg_path = dir.join("cimdea.toml");
        std::fs::write(&cfg_path, toml).unwrap();

        let cfg = AppConfig::load(&cfg_path).unwrap();
        assert_eq!(cfg.default_environment.as_deref(), Some("dev"));

        // prod: relative key file resolves against the config dir; absolute data_root stays.
        let prod = cfg.resolve(Some("prod")).unwrap();
        assert_eq!(prod.name, "prod");
        assert_eq!(prod.api_key_path, dir.join("prod_key.txt"));
        assert_eq!(prod.data_root, "/srv/ipums");
        assert_eq!(prod.read_api_key().unwrap(), "SECRET-KEY");

        // No argument falls back to the default (dev); ~ expands in its data_root.
        let default = cfg.resolve(None).unwrap();
        assert_eq!(default.name, "dev");
        if let Some(home) = home_dir() {
            assert_eq!(default.data_root, format!("{home}/dev_data"));
        }

        // Unknown environment is a clear error.
        assert!(cfg.resolve(Some("staging")).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
