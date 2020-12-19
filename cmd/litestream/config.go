package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v2"
)

// Config represents a configuration file for the litestream CLI.
type Config struct {
	DBs []*DBConfig `yaml:"databases"`
}

// DefaultConfig returns a new instance of Config with defaults set.
func DefaultConfig() Config {
	return Config{}
}

// ReadConfigFile unmarshals config from filename. Expands path if needed.
func ReadConfigFile(filename string) (Config, error) {
	config := DefaultConfig()

	// Expand filename, if necessary.
	if prefix := "~" + string(os.PathSeparator); strings.HasPrefix(filename, prefix) {
		u, err := user.Current()
		if err != nil {
			return config, err
		} else if u.HomeDir == "" {
			return config, fmt.Errorf("home directory unset")
		}
		filename = filepath.Join(u.HomeDir, strings.TrimPrefix(filename, prefix))
	}

	// Read & deserialize configuration.
	if buf, err := ioutil.ReadFile(filename); os.IsNotExist(err) {
		return config, fmt.Errorf("config file not found: %s", filename)
	} else if err != nil {
		return config, err
	} else if err := yaml.Unmarshal(buf, &config); err != nil {
		return config, err
	}
	return config, nil
}

type DBConfig struct {
	Path        string              `yaml:"path"`
	Replicators []*ReplicatorConfig `yaml:"replicators`
}

type ReplicatorConfig struct {
	Type string `yaml:"type"` // "file", "s3"
	Name string `yaml:"name"` // name of replicator, optional.
	Path string `yaml:"path"` // used for file replicators
}
