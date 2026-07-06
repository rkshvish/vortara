package sync

import (
	"os"
	"regexp"
)

var envRe = regexp.MustCompile(`\$\{([^}]+)\}`)

// applyEnv replaces ${VAR} placeholders with their environment values.
func applyEnv(f *SyncFile) {
	s := &f.Sync
	s.Name = expandEnv(s.Name)
	applyEnvSource(&s.Source)
	applyEnvDest(&s.Destination)
	s.State.Connection = expandEnv(s.State.Connection)
	s.State.Path = expandEnv(s.State.Path)
	s.Errors.DLQPath = expandEnv(s.Errors.DLQPath)
}

func applyEnvSource(s *SourceConfig) {
	s.URL = expandEnv(s.URL)
	s.Query = expandEnv(s.Query)
	if s.Auth != nil {
		applyEnvAuth(s.Auth)
	}
}

func applyEnvDest(d *DestinationConfig) {
	d.URL = expandEnv(d.URL)
	if d.Auth != nil {
		applyEnvAuth(d.Auth)
	}
	for k, v := range d.Options {
		d.Options[k] = expandEnv(v)
	}
}

func applyEnvAuth(a *AuthConfig) {
	a.Token = expandEnv(a.Token)
	a.ClientID = expandEnv(a.ClientID)
	a.ClientSecret = expandEnv(a.ClientSecret)
	a.TokenURL = expandEnv(a.TokenURL)
	a.Key = expandEnv(a.Key)
	a.Value = expandEnv(a.Value)
	a.Username = expandEnv(a.Username)
	a.Password = expandEnv(a.Password)
}

func expandEnv(s string) string {
	return envRe.ReplaceAllStringFunc(s, func(match string) string {
		key := match[2 : len(match)-1]
		if val, ok := os.LookupEnv(key); ok {
			return val
		}
		return match
	})
}
