package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile         = configFile("ca.pem")
	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")

	ClientCertFile       = configFile("client.pem")
	ClientKeyFile        = configFile("client-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NoBodyClientCertFile = configFile("nobody-client.pem")
	NoBodyClientKeyFile  = configFile("nobody-client-key.pem")
	defaultConfigDirEnv  = "CONFIG_DIR"
)

func configFile(filename string) string {
	if dir := os.Getenv(defaultConfigDirEnv); dir != "" {
		return filepath.Join(dir, filename)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}

	return filepath.Join(homeDir, ".sks", filename)
}
