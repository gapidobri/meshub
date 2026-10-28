package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// config is the backend's configuration. It is read from a YAML file (see
// meshub.example.yaml), then overridden by MESHUB_* environment variables
// (MESHUB_MQTT_PASSWORD for mqtt.password) and by flags.
type config struct {
	Log struct {
		Level  string `mapstructure:"level"`  // debug, info, warn or error
		Format string `mapstructure:"format"` // text or json
	} `mapstructure:"log"`
	DB struct {
		Path string `mapstructure:"path"`
	} `mapstructure:"db"`
	Retention struct {
		Packets time.Duration `mapstructure:"packets"`
		Events  time.Duration `mapstructure:"events"` // 0 keeps them
	} `mapstructure:"retention"`
	HTTP struct {
		Addr string `mapstructure:"addr"`
	} `mapstructure:"http"`
	API struct {
		Tokens []string `mapstructure:"tokens"`
	} `mapstructure:"api"`
	UI struct {
		Enabled  bool   `mapstructure:"enabled"`
		Username string `mapstructure:"username"`
		Password string `mapstructure:"password"`
	} `mapstructure:"ui"`
	MQTT struct {
		Broker   string `mapstructure:"broker"`
		ClientID string `mapstructure:"client_id"`
		Username string `mapstructure:"username"`
		Password string `mapstructure:"password"`
	} `mapstructure:"mqtt"`
	Hub struct {
		Name           string        `mapstructure:"name"`
		AdvertInterval time.Duration `mapstructure:"advert_interval"`
		AutoPair       bool          `mapstructure:"auto_pair"`
	} `mapstructure:"hub"`
	Apps struct {
		Enabled bool `mapstructure:"enabled"` // publish on the appproto topics
	} `mapstructure:"apps"`
}

// defaults lists every key, which also lets environment variables set keys
// that are in neither the file nor the flags.
var defaults = map[string]any{
	"log.level":           "info",
	"log.format":          "text",
	"db.path":             "meshub.db",
	"retention.packets":   7 * 24 * time.Hour,
	"retention.events":    time.Duration(0),
	"http.addr":           ":8080",
	"api.tokens":          []string{},
	"ui.enabled":          true,
	"ui.username":         "admin",
	"ui.password":         "",
	"mqtt.broker":         "tcp://localhost:1883",
	"mqtt.client_id":      "",
	"mqtt.username":       "",
	"mqtt.password":       "",
	"hub.name":            "meshub",
	"hub.advert_interval": 12 * time.Hour,
	"hub.auto_pair":       false,
	"apps.enabled":        true,
}

// configPaths are where the config file is looked for without --config.
var configPaths = []string{"meshub.yaml", "/etc/meshub/meshub.yaml"}

// flagKeys maps flags to the config keys they set.
var flagKeys = map[string]string{
	"db":        "db.path",
	"http":      "http.addr",
	"mqtt":      "mqtt.broker",
	"log-level": "log.level",
	"auto-pair": "hub.auto_pair",
}

// addConfigFlags adds the config file flag, and the flags that override
// common settings.
func addConfigFlags(fs *pflag.FlagSet) {
	fs.StringP("config", "c", "", "config file (default: ./meshub.yaml or /etc/meshub/meshub.yaml, if present; $MESHUB_CONFIG)")
	fs.String("db", "", "SQLite database path; it holds the hub's private key (db.path)")
	fs.String("http", "", "HTTP listen address (http.addr)")
	fs.String("mqtt", "", "MQTT broker URL (mqtt.broker)")
	fs.String("log-level", "", "debug, info, warn or error (log.level)")
	fs.Bool("auto-pair", false, "accept every device without approval, for development (hub.auto_pair)")
}

// loadConfig reads the configuration for a command with addConfigFlags.
func loadConfig(fs *pflag.FlagSet) (config, error) {
	v := viper.New()
	for k, d := range defaults {
		v.SetDefault(k, d)
	}
	v.SetEnvPrefix("MESHUB")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	for name, key := range flagKeys {
		if err := v.BindPFlag(key, fs.Lookup(name)); err != nil {
			return config{}, err
		}
	}

	path, _ := fs.GetString("config")
	if path == "" {
		path = os.Getenv("MESHUB_CONFIG")
	}
	if path == "" {
		// Not viper's search by name, which also matches files without an
		// extension, such as the meshub binary itself.
		for _, p := range configPaths {
			if _, err := os.Stat(p); err == nil {
				path = p
				break
			}
		}
	}
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return config{}, fmt.Errorf("config: %w", err)
		}
	}

	var c config
	if err := v.Unmarshal(&c); err != nil {
		return c, fmt.Errorf("config: %w", err)
	}
	// A list from the environment arrives as one string.
	if len(c.API.Tokens) == 1 && strings.Contains(c.API.Tokens[0], ",") {
		c.API.Tokens = strings.Split(c.API.Tokens[0], ",")
	}
	c.API.Tokens = nonEmpty(c.API.Tokens)
	return c, c.validate()
}

func (c config) validate() error {
	var errs []error
	if _, err := logLevel(c.Log.Level); err != nil {
		errs = append(errs, err)
	}
	if c.Log.Format != "text" && c.Log.Format != "json" {
		errs = append(errs, fmt.Errorf("log.format must be text or json, not %q", c.Log.Format))
	}
	if c.DB.Path == "" {
		errs = append(errs, errors.New("db.path is required"))
	}
	if c.Retention.Packets <= 0 {
		errs = append(errs, errors.New("retention.packets must be positive"))
	}
	if c.Retention.Events < 0 {
		errs = append(errs, errors.New("retention.events must not be negative"))
	}
	if c.Hub.Name == "" {
		errs = append(errs, errors.New("hub.name is required"))
	}
	if c.Hub.AdvertInterval < 0 {
		errs = append(errs, errors.New("hub.advert_interval must not be negative"))
	}
	if c.UI.Enabled && c.UI.Password != "" && c.UI.Username == "" {
		errs = append(errs, errors.New("ui.username is required with ui.password"))
	}
	for _, t := range c.API.Tokens {
		if len(t) < 16 {
			errs = append(errs, errors.New("api.tokens must be at least 16 characters each"))
			break
		}
	}
	return errors.Join(errs...)
}

func logLevel(s string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return l, fmt.Errorf("log.level: %w", err)
	}
	return l, nil
}

func (c config) logger() *slog.Logger {
	level, _ := logLevel(c.Log.Level)
	opts := &slog.HandlerOptions{Level: level}
	if c.Log.Format == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func nonEmpty(ss []string) []string {
	out := ss[:0]
	for _, s := range ss {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
