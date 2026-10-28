package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

func flags(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	addConfigFlags(fs)
	if err := fs.Parse(args); err != nil {
		t.Fatal(err)
	}
	return fs
}

func TestConfigDefaults(t *testing.T) {
	t.Chdir(t.TempDir()) // no meshub.yaml
	c, err := loadConfig(flags(t))
	if err != nil {
		t.Fatal(err)
	}
	if c.DB.Path != "meshub.db" || c.HTTP.Addr != ":8080" || c.Hub.AdvertInterval != 12*time.Hour ||
		!c.UI.Enabled || !c.Apps.Enabled || len(c.API.Tokens) != 0 {
		t.Errorf("defaults = %+v", c)
	}
}

func TestConfigPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meshub.yaml")
	os.WriteFile(path, []byte(`
db:
  path: /var/lib/meshub/meshub.db
http:
  addr: ":9000"
hub:
  name: weathers
  advert_interval: 6h
mqtt:
  broker: tcp://broker:1883
api:
  tokens: [aaaaaaaaaaaaaaaa]
`), 0o600)
	t.Setenv("MESHUB_MQTT_PASSWORD", "secret")
	t.Setenv("MESHUB_HTTP_ADDR", ":9100")
	t.Setenv("MESHUB_RETENTION_EVENTS", "720h")

	c, err := loadConfig(flags(t, "--config", path, "--http", ":9200"))
	if err != nil {
		t.Fatal(err)
	}
	if c.DB.Path != "/var/lib/meshub/meshub.db" || c.Hub.Name != "weathers" || c.Hub.AdvertInterval != 6*time.Hour {
		t.Errorf("from file: %+v", c)
	}
	if c.MQTT.Password != "secret" || c.Retention.Events != 720*time.Hour {
		t.Errorf("from env: %+v", c)
	}
	if c.HTTP.Addr != ":9200" {
		t.Errorf("http.addr = %q, want the flag's", c.HTTP.Addr)
	}
	if len(c.API.Tokens) != 1 {
		t.Errorf("tokens = %v", c.API.Tokens)
	}

	t.Setenv("MESHUB_API_TOKENS", "bbbbbbbbbbbbbbbb, cccccccccccccccc")
	if c, err = loadConfig(flags(t, "--config", path)); err != nil || len(c.API.Tokens) != 2 || c.API.Tokens[1] != "cccccccccccccccc" {
		t.Errorf("tokens from env = %q, %v", c.API.Tokens, err)
	}
}

func TestConfigInvalid(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("MESHUB_LOG_FORMAT", "xml")
	t.Setenv("MESHUB_API_TOKENS", "short")
	_, err := loadConfig(flags(t))
	if err == nil || !strings.Contains(err.Error(), "log.format") || !strings.Contains(err.Error(), "api.tokens") {
		t.Errorf("err = %v", err)
	}
	if _, err := loadConfig(flags(t, "--config", "missing.yaml")); err == nil {
		t.Error("missing config file accepted")
	}
}

// TestOpenAPIUpToDate keeps docs/openapi.yaml in step with the API; run
// make openapi to update it.
func TestOpenAPIUpToDate(t *testing.T) {
	want, err := os.ReadFile("../../../docs/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var got strings.Builder
	cmd := rootCmd()
	cmd.SetOut(&got)
	cmd.SetArgs([]string{"openapi"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got.String() != string(want) {
		t.Error("docs/openapi.yaml is out of date: run make openapi")
	}
}

func TestBackup(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "meshub.db")
	run := func(args ...string) (string, error) {
		var out strings.Builder
		cmd := rootCmd()
		cmd.SetOut(&out)
		cmd.SetErr(&out)
		cmd.SetArgs(args)
		err := cmd.Execute()
		return out.String(), err
	}
	if _, err := run("backup", "--db", db); err == nil {
		t.Error("backup of a missing database succeeded")
	}
	id, err := run("identity", "--db", db)
	if err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(dir, "copy.db")
	out, err := run("backup", "--db", db, dest)
	if err != nil || !strings.Contains(out, "hub "+strings.TrimSpace(id)) {
		t.Errorf("backup = %q, %v; want hub %s", out, err, id)
	}
	if _, err := run("backup", "--db", db, dest); err == nil {
		t.Error("backup over an existing file succeeded")
	}
	if restored, _ := run("identity", "--db", dest); restored != id {
		t.Errorf("identity of the copy = %q, want %q", restored, id)
	}
}
