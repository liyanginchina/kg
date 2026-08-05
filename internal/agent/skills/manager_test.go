package skills

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/sandbox"
)

// fakeSandbox records the last ExecuteConfig it received so tests can assert
// how ExecuteScript assembled args/stdin. It also captures the temp input file
// content DURING execution (before ExecuteScript's deferred cleanup deletes it).
type fakeSandbox struct {
	lastConfig    *sandbox.ExecuteConfig
	capturedInput string
}

func (f *fakeSandbox) Execute(_ context.Context, config *sandbox.ExecuteConfig) (*sandbox.ExecuteResult, error) {
	f.lastConfig = config
	// Reconstruct and read the temp input file (created in the scripts dir)
	// while it still exists, to prove the script would be able to read it.
	for _, a := range config.Args {
		if strings.HasPrefix(a, ".skill_input_") {
			tmpPath := filepath.Join(filepath.Dir(config.Script), a)
			if b, err := os.ReadFile(tmpPath); err == nil {
				f.capturedInput = string(b)
			}
		}
	}
	return &sandbox.ExecuteResult{ExitCode: 0}, nil
}
func (f *fakeSandbox) Cleanup(_ context.Context) error      { return nil }
func (f *fakeSandbox) GetSandbox() sandbox.Sandbox           { return nil }
func (f *fakeSandbox) GetType() sandbox.SandboxType          { return sandbox.SandboxTypeLocal }

// setupTestSkill creates a temp skill dir with a SKILL.md and a script, then
// returns a Manager wired to a fake sandbox.
func setupTestSkill(t *testing.T) (*Manager, *fakeSandbox, string) {
	t.Helper()
	root := t.TempDir()
	skillDir := filepath.Join(root, "mysk")
	scriptsDir := filepath.Join(skillDir, "scripts")
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	skillMd := "---\nname: mysk\ndescription: test skill\n---\n# mysk\n"
	if err := os.WriteFile(filepath.Join(skillDir, "SKILL.md"), []byte(skillMd), 0o644); err != nil {
		t.Fatalf("write skill: %v", err)
	}
	if err := os.WriteFile(filepath.Join(scriptsDir, "run.py"), []byte("print('hi')\n"), 0o644); err != nil {
		t.Fatalf("write script: %v", err)
	}

	loader := NewLoader([]string{root})
	if _, err := loader.DiscoverSkills(); err != nil {
		t.Fatalf("discover: %v", err)
	}
	fake := &fakeSandbox{}
	m := &Manager{
		loader:     loader,
		sandboxMgr: fake,
		skillDirs:  []string{root},
		enabled:    true,
	}
	return m, fake, scriptsDir
}

func TestExecuteScript_InputAsFile(t *testing.T) {
	m, fake, _ := setupTestSkill(t)
	const data = "SELECT 1 FROM dual"
	if _, err := m.ExecuteScript(context.Background(), "mysk", "scripts/run.py", nil, data, true); err != nil {
		t.Fatalf("ExecuteScript err: %v", err)
	}
	if fake.lastConfig == nil {
		t.Fatal("sandbox Execute not called")
	}
	if fake.lastConfig.Stdin != "" {
		t.Fatalf("expected stdin cleared when input_as_file=true, got %q", fake.lastConfig.Stdin)
	}
	if len(fake.lastConfig.Args) != 1 {
		t.Fatalf("expected exactly 1 arg (temp file), got %v", fake.lastConfig.Args)
	}
	tmpName := fake.lastConfig.Args[0]
	if !strings.HasPrefix(tmpName, ".skill_input_") {
		t.Fatalf("expected temp file arg, got %q", tmpName)
	}
	// The script dir is bind-mounted into the sandbox; the fake sandbox read the
	// temp file during execution (before the deferred cleanup), proving the
	// script would be able to open it by its base name.
	if fake.capturedInput != data {
		t.Fatalf("captured temp file content mismatch: got %q want %q", fake.capturedInput, data)
	}
}

func TestExecuteScript_StdinMode(t *testing.T) {
	m, fake, _ := setupTestSkill(t)
	const data = "SELECT 2 FROM dual"
	if _, err := m.ExecuteScript(context.Background(), "mysk", "scripts/run.py", []string{"--flag"}, data, false); err != nil {
		t.Fatalf("ExecuteScript err: %v", err)
	}
	if fake.lastConfig.Stdin != data {
		t.Fatalf("expected stdin == data, got %q", fake.lastConfig.Stdin)
	}
	if len(fake.lastConfig.Args) != 1 || fake.lastConfig.Args[0] != "--flag" {
		t.Fatalf("expected args unchanged (only --flag), got %v", fake.lastConfig.Args)
	}
}

func TestExecuteScript_InputAsFileEmptyStdin(t *testing.T) {
	m, fake, _ := setupTestSkill(t)
	if _, err := m.ExecuteScript(context.Background(), "mysk", "scripts/run.py", nil, "", true); err != nil {
		t.Fatalf("ExecuteScript err: %v", err)
	}
	if len(fake.lastConfig.Args) != 0 {
		t.Fatalf("expected no args when stdin empty, got %v", fake.lastConfig.Args)
	}
	if fake.lastConfig.Stdin != "" {
		t.Fatalf("expected empty stdin, got %q", fake.lastConfig.Stdin)
	}
}

func TestWriteTempInputFile(t *testing.T) {
	dir := t.TempDir()
	p, err := writeTempInputFile(dir, "hello world")
	if err != nil {
		t.Fatalf("writeTempInputFile err: %v", err)
	}
	defer os.Remove(p)
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("content mismatch: %q", string(got))
	}
	if filepath.Dir(p) != dir {
		t.Fatalf("expected file in %s, got %s", dir, p)
	}
	if !strings.HasPrefix(filepath.Base(p), ".skill_input_") {
		t.Fatalf("unexpected temp file name: %s", filepath.Base(p))
	}
}
