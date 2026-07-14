use super::*;
use pretty_assertions::assert_eq;

#[test]
fn codex_home_is_confined_to_configured_root() {
    let config = test_config();

    assert_eq!(
        config.codex_home("home_07").unwrap(),
        PathBuf::from("/codex-home/home_07")
    );
    assert!(config.codex_home("../other").is_err());
    assert!(config.codex_home("nested/shard").is_err());
}

fn test_config() -> Config {
    Config {
        control_plane_url: "http://control-plane".to_string(),
    }
}
