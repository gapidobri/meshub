// Command meshub is the meshub backend: it ingests gateway traffic from MQTT
// into SQLite, serves devices on the mesh, and serves the web UI and the
// HTTP API for applications.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/gapidobri/meshub/server/internal/api"
	"github.com/gapidobri/meshub/server/internal/hub"
	"github.com/gapidobri/meshub/server/internal/store"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "meshub",
		Short:        "The meshub backend",
		Version:      fullVersion(),
		SilenceUsage: true,
	}
	root.AddCommand(serveCmd(), openapiCmd(), identityCmd(), backupCmd(), versionCmd(), healthcheckCmd())
	return root
}

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the backend",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(cmd.Flags())
			if err != nil {
				return err
			}
			return serve(cmd.Context(), cfg, cfg.logger())
		},
	}
	addConfigFlags(cmd.Flags())
	return cmd
}

func openapiCmd() *cobra.Command {
	var format string
	cmd := &cobra.Command{
		Use:   "openapi",
		Short: "Print the HTTP API's OpenAPI description",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			log := slog.New(slog.NewTextHandler(io.Discard, nil))
			doc := api.Register(http.NewServeMux(), nil, nil, log, api.Options{Version: version}).OpenAPI()
			var out []byte
			var err error
			switch format {
			case "yaml":
				out, err = doc.YAML()
			case "json":
				out, err = json.MarshalIndent(doc, "", "  ")
				out = append(out, '\n')
			default:
				err = fmt.Errorf("unknown format %q", format)
			}
			if err != nil {
				return err
			}
			_, err = cmd.OutOrStdout().Write(out)
			return err
		},
	}
	cmd.Flags().StringVarP(&format, "format", "f", "yaml", "yaml or json")
	return cmd
}

func identityCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "identity",
		Short: "Print the hub's public key, which devices are built with",
		Long:  "Print the hub's public key, which devices are built with. The identity is created on first use.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, err := loadConfig(cmd.Flags())
			if err != nil {
				return err
			}
			st, err := store.Open(cfg.DB.Path)
			if err != nil {
				return err
			}
			defer st.Close()
			id, err := hub.LoadIdentity(context.Background(), st)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), id.String())
			return nil
		},
	}
	addConfigFlags(cmd.Flags())
	return cmd
}

// healthcheckCmd checks a running backend, for container health checks in
// images without a shell.
func healthcheckCmd() *cobra.Command {
	var url string
	cmd := &cobra.Command{
		Use:    "healthcheck",
		Short:  "Exit with an error unless the backend at --url is healthy",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			c := http.Client{Timeout: 5 * time.Second}
			resp, err := c.Get(url)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
				return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&url, "url", "http://127.0.0.1:8080/healthz", "URL to check; /readyz also requires the database and broker")
	return cmd
}

func backupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup [file]",
		Short: "Copy the database, with the hub's private key, while the backend runs",
		Long: "Copy the database, with the hub's private key, while the backend runs. Without a file, the copy is\n" +
			"meshub-backup-{time}.db next to the database. The copy is a database like the original: restore it\n" +
			"by stopping the backend and putting it in place of db.path.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig(cmd.Flags())
			if err != nil {
				return err
			}
			if _, err := os.Stat(cfg.DB.Path); err != nil {
				return fmt.Errorf("database: %w", err)
			}
			dest := filepath.Join(filepath.Dir(cfg.DB.Path), "meshub-backup-"+time.Now().UTC().Format("20060102-150405")+".db")
			if len(args) == 1 {
				dest = args[0]
			}
			ctx := cmd.Context()
			st, err := store.Open(cfg.DB.Path)
			if err != nil {
				return err
			}
			defer st.Close()
			if err := st.Backup(ctx, dest); err != nil {
				return fmt.Errorf("backup: %w", err)
			}

			// Check the copy holds the identity devices are built with.
			cp, err := store.Open(dest)
			if err != nil {
				return fmt.Errorf("backup unreadable: %w", err)
			}
			defer cp.Close()
			id, ok, err := hub.StoredIdentity(ctx, cp)
			if err != nil {
				return fmt.Errorf("backup unreadable: %w", err)
			}
			if !ok {
				return errors.New("backup has no hub identity: the backend never ran on this database")
			}
			fmt.Fprintf(cmd.OutOrStdout(), "%s\nhub %s\n", dest, id.String())
			return nil
		},
	}
	addConfigFlags(cmd.Flags())
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintln(cmd.OutOrStdout(), fullVersion())
		},
	}
}

// fullVersion is the version with the VCS revision it was built from.
func fullVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	var rev, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value[:min(12, len(s.Value))]
		case "vcs.modified":
			if s.Value == "true" {
				modified = "-dirty"
			}
		}
	}
	if rev == "" {
		return version
	}
	return version + " (" + rev + modified + ")"
}
