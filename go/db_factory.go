// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trino

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/trinodb/trino-go-client/trino"
)

// TrinoDBFactory provides Trino-specific database connection creation.
// It handles Trino DSN formatting and connection parameters.
type TrinoDBFactory struct{}

// NewTrinoDBFactory creates a new TrinoDBFactory.
func NewTrinoDBFactory() *TrinoDBFactory {
	return &TrinoDBFactory{}
}

// CreateDB creates a *sql.DB using sql.Open with a Trino-specific DSN.
func (f *TrinoDBFactory) CreateDB(ctx context.Context, driverName string, opts map[string]string, logger *slog.Logger) (*sql.DB, error) {
	dsn, err := f.BuildTrinoDSN(opts)
	if err != nil {
		return nil, err
	}

	// Register custom HTTP client to prevent infrastructure timeouts
	dsn, err = f.registerCustomClientForTimeout(dsn)
	if err != nil {
		return nil, err
	}

	return sql.Open(driverName, dsn)
}

// registerCustomClientForTimeout creates and registers a custom HTTP client
// This prevents infrastructure/network timeouts during long-running queries
func (f *TrinoDBFactory) registerCustomClientForTimeout(dsn string) (string, error) {
	// Parse DSN URL to check for SSLVerification parameter
	// Note: Must use url.Parse since SSLVerification parameter does not exist in trino.ParseDSN
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("failed to parse DSN URL: %v", err)
	}
	skipVerification := strings.EqualFold(parsedURL.Query().Get("SSLVerification"), "NONE")

	cfg, err := trino.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("failed to parse DSN: %v", err)
	}

	timeout := trino.DefaultQueryTimeout
	if cfg.QueryTimeout != nil {
		timeout = *cfg.QueryTimeout
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = timeout

	tlsConfig, err := buildTLSConfig(transport.TLSClientConfig, cfg, skipVerification)
	if err != nil {
		return "", err
	}
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}

	httpClientName := customClientName(timeout, skipVerification, cfg.SSLCertPath, cfg.SSLCert)
	customClient := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
	if err := trino.RegisterCustomClient(httpClientName, customClient); err != nil {
		return "", fmt.Errorf("failed to register custom HTTP client: %v", err)
	}

	cfg.CustomClientName = httpClientName
	// The custom HTTP client now owns TLS verification/trust configuration.
	cfg.SSLCertPath = ""
	cfg.SSLCert = ""

	return cfg.FormatDSN()
}

func buildTLSConfig(base *tls.Config, cfg *trino.Config, skipVerification bool) (*tls.Config, error) {
	tlsConfig := cloneTLSConfig(base)

	if skipVerification {
		tlsConfig.InsecureSkipVerify = true
	}

	if cfg.SSLCert != "" || cfg.SSLCertPath != "" {
		cert, err := loadSSLCert(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load SSL certificate: %v", err)
		}

		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(cert) {
			return nil, fmt.Errorf("failed to parse SSL certificate")
		}
		tlsConfig.RootCAs = certPool
	}

	if skipVerification || cfg.SSLCert != "" || cfg.SSLCertPath != "" {
		return tlsConfig, nil
	}

	return nil, nil
}

func cloneTLSConfig(base *tls.Config) *tls.Config {
	if base == nil {
		return &tls.Config{}
	}
	return base.Clone()
}

func loadSSLCert(cfg *trino.Config) ([]byte, error) {
	if cfg.SSLCert != "" {
		return []byte(cfg.SSLCert), nil
	}
	return os.ReadFile(cfg.SSLCertPath)
}

func customClientName(timeout time.Duration, skipVerification bool, sslCertPath, sslCert string) string {
	sum := sha256.Sum256(fmt.Appendf(nil,
		"timeout=%s|skip_verification=%t|ssl_cert_path=%s|ssl_cert=%s",
		timeout, skipVerification, sslCertPath, sslCert,
	))
	return fmt.Sprintf("adbc_trino_timeout_%x", sum[:8])
}

// buildTrinoDSN constructs a Trino DSN from the provided options.
// Handles the following scenarios:
//  1. Trino URI: "trino://user:pass@host:port/catalog/schema?params" → converted to DSN
//  2. Full DSN, no separate credentials:
//     Example: "https://user:pass@localhost:8080?catalog=default"
//     → Returned as-is.
//  3. Plain host + credentials:
//     Example: baseURI="localhost:8080", username="user", password="secret"
//     → Produces "https://user:secret@localhost:8080".
//  4. Full DSN + override credentials:
//     Example: baseURI="https://old:old@localhost:8080?catalog=default", username="new", password="newpass"
//     → Credentials are replaced.
func (f *TrinoDBFactory) BuildTrinoDSN(opts map[string]string) (string, error) {
	baseURI := opts[adbc.OptionKeyURI]
	username := opts[adbc.OptionKeyUsername]
	password := opts[adbc.OptionKeyPassword]

	// If no base URI provided, this is an error
	if baseURI == "" {
		// Return plain Go error. sqlwrapper will catch and wrap it with ErrorHelper and turn it into adbc error
		return "", fmt.Errorf("missing required option %s", adbc.OptionKeyURI)
	}

	if strings.HasPrefix(baseURI, "trino://") {
		return f.parseTrinoURIToDSN(baseURI, username, password)
	}

	return f.buildDSNFromHTTP(baseURI, username, password)
}

// parseTrinoURIToDSN converts a Trino URI to Trino DSN format using pure URL manipulation.
// Examples:
//
//	trino://localhost:8080/hive/default → http://localhost:8080?catalog=hive&schema=default
//	trino://user:pass@host:8080/postgresql/public → http://user:pass@host:8080?catalog=postgresql&schema=public
//	trino://user@host/memory/default?SSL=true → https://user@host:443?catalog=memory&schema=default&SSL=true
func (f *TrinoDBFactory) parseTrinoURIToDSN(trinoURI, username, password string) (string, error) {
	u, err := url.Parse(trinoURI)
	if err != nil {
		return "", fmt.Errorf("invalid Trino URI format: %v", err)
	}

	queryParams := u.Query()

	scheme := "https"
	if strings.EqualFold(queryParams.Get("SSL"), "false") {
		scheme = "http"
	}

	if path := strings.TrimPrefix(u.Path, "/"); path != "" {
		parts := strings.SplitN(path, "/", 2)
		queryParams.Set("catalog", parts[0])
		if len(parts) == 2 && parts[1] != "" {
			queryParams.Set("schema", parts[1])
		}
	}

	dsn := &url.URL{
		Scheme:   scheme,
		User:     f.applyCredentialOverrides(u.User, username, password),
		Host:     f.ensureHostPort(u, scheme),
		RawQuery: queryParams.Encode(),
	}

	return dsn.String(), nil
}

// ensureHostPort handles host and port assignment
func (f *TrinoDBFactory) ensureHostPort(u *url.URL, scheme string) string {
	if u.Port() != "" {
		return u.Host
	}
	defaultPort := "8443"
	if scheme == "http" {
		defaultPort = "8080"
	}
	return u.Host + ":" + defaultPort
}

// buildDSNFromHTTP handles existing HTTP/HTTPS DSNs and plain host strings.
func (f *TrinoDBFactory) buildDSNFromHTTP(baseURI, username, password string) (string, error) {
	if !strings.HasPrefix(baseURI, "http://") && !strings.HasPrefix(baseURI, "https://") {

		scheme := "https://"
		if strings.Contains(baseURI, "SSL=false") {
			scheme = "http://"
		}
		baseURI = scheme + baseURI
	}

	u, err := url.Parse(baseURI)
	if err != nil {
		return "", fmt.Errorf("invalid DSN format: %v", err)
	}

	u.User = f.applyCredentialOverrides(u.User, username, password)

	return u.String(), nil
}

// applyCredentialOverrides contains username and password override logic
func (f *TrinoDBFactory) applyCredentialOverrides(existing *url.Userinfo, username, password string) *url.Userinfo {
	if username == "" && password == "" {
		return existing
	}

	user := ""
	pass := ""
	hasPass := false

	if existing != nil {
		user = existing.Username()
		pass, hasPass = existing.Password()
	}

	if username != "" {
		user = username
	}
	if password != "" {
		pass = password
		hasPass = true
	}

	if hasPass {
		return url.UserPassword(user, pass)
	}
	return url.User(user)
}

// Ensure TrinoDBFactory implements sqlwrapper.DBFactory
var _ sqlwrapper.DBFactory = (*TrinoDBFactory)(nil)
