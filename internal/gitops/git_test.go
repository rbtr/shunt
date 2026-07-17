package gitops

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitCommandAllowsAskpassWhenAmbientConfigDisablesPrompts(t *testing.T) {
	const token = "example"

	home := t.TempDir()
	globalConfig := filepath.Join(home, ".gitconfig")
	if err := os.WriteFile(globalConfig, nil, 0o600); err != nil {
		t.Fatalf("write global git config: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(home, ".config"))
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", globalConfig)
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "credential.interactive")
	t.Setenv("GIT_CONFIG_VALUE_0", "never")

	authDir := t.TempDir()
	askpass := filepath.Join(authDir, "askpass.sh")
	if err := os.WriteFile(askpass, []byte(askpassScript), 0o700); err != nil {
		t.Fatalf("write askpass: %v", err)
	}
	tokenFile := filepath.Join(authDir, "token")
	if err := os.WriteFile(tokenFile, []byte(token), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	cmd := gitCommand(t.Context(), authDir, gitAuth{
		askpass:   askpass,
		user:      "test-user",
		tokenFile: tokenFile,
	}, "credential", "fill")
	cmd.Stdin = strings.NewReader("protocol=https\nhost=example.invalid\n\n")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git credential fill: %v", err)
	}
	fields := make(map[string]string)
	for _, line := range strings.Split(string(out), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			fields[key] = value
		}
	}
	if got := fields["username"]; got != "test-user" {
		t.Fatalf("credential username = %q, want test-user", got)
	}
	if got := fields["password"]; got != token {
		t.Fatal("git credential fill did not return the askpass password")
	}
}
