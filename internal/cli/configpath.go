package cli

import "os"

// EnvConfigPath is the environment variable consulted when --config is not set.
const EnvConfigPath = "STORMAN_CONFIG_PATH"

// DefaultConfigPath is the fallback when neither --config nor $STORMAN_CONFIG_PATH is set.
// Lines up with the data dir mounted by deployments/docker-compose.yml.
const DefaultConfigPath = "/var/lib/storman/config.json"

// resolveConfigPath picks the config path with --config > $STORMAN_CONFIG_PATH > default.
func resolveConfigPath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if v := os.Getenv(EnvConfigPath); v != "" {
		return v
	}
	return DefaultConfigPath
}
