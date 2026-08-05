package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/sandbox"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/utils"
)

// bashTool describes the bash tool. The command runs inside the sandbox in a
// session-scoped temporary working directory (workDir) and is subject to the
// sandbox validator's deny-list / command whitelist.
var bashTool = BaseTool{
	name: ToolBash,
	description: `Execute a shell command inside a sandboxed environment.

## Usage
- Run a shell command (bash) in an isolated sandbox.
- The working directory is a session-scoped temporary directory. All file
  access is restricted to this directory and its subdirectories.
- Network access is disabled; commands are subject to a deny-list that blocks
  dangerous operations (e.g. curl/wget/rm -rf/reverse shells).

## When to Use
- When you need to inspect, transform, or generate files produced within the
  session workspace (e.g. list files, run awk/sed/grep over a generated file).
- When a skill or workflow requires deterministic shell operations.

## Security
- Runs inside the sandbox (Docker isolation when available) with a deny-list.
- Only session-scoped temporary files are visible.

## Returns
- Combined stdout/stderr and the exit code of the command.`,
	schema: utils.GenerateSchema[BashToolInput](),
}

// BashToolInput defines the input parameters for the bash tool.
type BashToolInput struct {
	Command string `json:"command" jsonschema:"The shell command to execute (bash syntax). Runs in the sandbox with a deny-list and no network."`
}

// BashTool allows the agent to execute a shell command in the sandbox.
type BashTool struct {
	BaseTool
	sandbox sandbox.Manager
	workDir string
}

// NewBashTool creates a new bash tool instance bound to a sandbox and a
// session-scoped working directory.
func NewBashTool(sb sandbox.Manager, workDir string) *BashTool {
	return &BashTool{
		BaseTool: bashTool,
		sandbox:  sb,
		workDir:  workDir,
	}
}

// Execute runs the command in the sandbox.
func (t *BashTool) Execute(ctx context.Context, args json.RawMessage) (*types.ToolResult, error) {
	var input BashToolInput
	if err := json.Unmarshal(args, &input); err != nil {
		return &types.ToolResult{Success: false, Error: fmt.Sprintf("Failed to parse args: %v", err)}, nil
	}
	if strings.TrimSpace(input.Command) == "" {
		return &types.ToolResult{Success: false, Error: "command is required"}, nil
	}
	if t.sandbox == nil || t.sandbox.GetType() == sandbox.SandboxTypeDisabled {
		return &types.ToolResult{Success: false, Error: "sandbox is disabled, bash tool is unavailable"}, nil
	}

	scriptPath, err := t.writeScript(input.Command)
	if err != nil {
		return &types.ToolResult{Success: false, Error: fmt.Sprintf("failed to prepare script: %v", err)}, nil
	}

	cfg := &sandbox.ExecuteConfig{
		Script:     scriptPath,
		WorkDir:    t.workDir,
		Timeout:    60 * time.Second,
		WritableDir: t.workDir,
	}
	return t.runAndFormat(ctx, cfg, input.Command)
}

// writeScript writes the command to a temp .sh file inside the session workdir.
func (t *BashTool) writeScript(command string) (string, error) {
	if err := os.MkdirAll(t.workDir, 0o777); err != nil {
		return "", err
	}
	scriptPath := filepath.Join(t.workDir, fmt.Sprintf("bash_%d.sh", time.Now().UnixNano()))
	// 0644 so the sandbox container user (uid 1000) can read the script even
	// when the host process runs as a different user.
	if err := os.WriteFile(scriptPath, []byte("#!/bin/bash\nset -e\n"+command+"\n"), 0o644); err != nil {
		return "", err
	}
	return scriptPath, nil
}

// runAndFormat executes the config and formats the result for the LLM.
func (t *BashTool) runAndFormat(ctx context.Context, cfg *sandbox.ExecuteConfig, label string) (*types.ToolResult, error) {
	result, err := t.sandbox.Execute(ctx, cfg)
	if err != nil {
		logger.Errorf(ctx, "[Tool][Bash] execution failed: %v", err)
		return &types.ToolResult{Success: false, Error: fmt.Sprintf("execution failed: %v", err)}, nil
	}

	var b strings.Builder
	b.WriteString("=== Command ===\n```bash\n" + label + "\n```\n\n")
	b.WriteString(fmt.Sprintf("**Exit Code**: %d\n", result.ExitCode))
	if result.Killed {
		b.WriteString("**Warning**: command was terminated (timeout or killed)\n\n")
	}
	if result.Stdout != "" {
		b.WriteString("## Standard Output\n\n```\n")
		b.WriteString(result.Stdout)
		if !strings.HasSuffix(result.Stdout, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("```\n\n")
	}
	if result.Stderr != "" {
		b.WriteString("## Standard Error\n\n```\n")
		b.WriteString(result.Stderr)
		if !strings.HasSuffix(result.Stderr, "\n") {
			b.WriteString("\n")
		}
		b.WriteString("```\n\n")
	}

	return &types.ToolResult{
		Success: result.IsSuccess(),
		Output:  b.String(),
		Data: map[string]interface{}{
			"exit_code":   result.ExitCode,
			"stdout":      result.Stdout,
			"stderr":      result.Stderr,
			"duration_ms": result.Duration.Milliseconds(),
			"killed":      result.Killed,
		},
		Error: func() string {
			if !result.IsSuccess() {
				if result.Error != "" {
					return result.Error
				}
				return fmt.Sprintf("command exited with code %d", result.ExitCode)
			}
			return ""
		}(),
	}, nil
}

// Cleanup implements types.Tool.
func (t *BashTool) Cleanup(ctx context.Context) error {
	return nil
}

// writeFileTool describes the write_file tool. It writes a file inside the
// session-scoped temporary working directory. The path is validated to prevent
// escaping the sandbox workspace; content is passed via stdin so it is not
// misinterpreted by the static script validator.
var writeFileTool = BaseTool{
	name: ToolWriteFile,
	description: `Write a text file inside the sandboxed session workspace.

## Usage
- Create or overwrite a file at a relative path within the session workspace.
- Absolute paths and paths that escape the workspace (e.g. containing "..") are
  rejected.
- The file lives only for the current session and is not shared across sessions.

## When to Use
- When a skill or workflow needs to persist intermediate data, config, or a
  report as a file for later inspection or downstream tooling.
- When generating a file (e.g. a script, a CSV, a markdown report) in the sandbox.

## Security
- Restricted to the session-scoped temporary directory; no access outside it.
- Runs through the sandbox (Docker isolation when available).

## Returns
- Confirmation of the write, the final path, and the byte size written.`,
	schema: utils.GenerateSchema[WriteFileToolInput](),
}

// WriteFileToolInput defines the input parameters for the write_file tool.
type WriteFileToolInput struct {
	Path    string `json:"path" jsonschema:"Relative path of the file to write within the session workspace (e.g. 'output/report.md'). Must not escape the workspace."`
	Content string `json:"content" jsonschema:"The full text content to write to the file."`
}

// WriteFileTool writes files inside the sandbox session workspace.
type WriteFileTool struct {
	BaseTool
	sandbox sandbox.Manager
	workDir string
}

// NewWriteFileTool creates a new write_file tool bound to a sandbox and a
// session-scoped working directory.
func NewWriteFileTool(sb sandbox.Manager, workDir string) *WriteFileTool {
	return &WriteFileTool{
		BaseTool: writeFileTool,
		sandbox:  sb,
		workDir:  workDir,
	}
}

// Execute writes the file inside the sandbox.
func (t *WriteFileTool) Execute(ctx context.Context, args json.RawMessage) (*types.ToolResult, error) {
	var input WriteFileToolInput
	if err := json.Unmarshal(args, &input); err != nil {
		return &types.ToolResult{Success: false, Error: fmt.Sprintf("Failed to parse args: %v", err)}, nil
	}
	if strings.TrimSpace(input.Path) == "" {
		return &types.ToolResult{Success: false, Error: "path is required"}, nil
	}
	if t.sandbox == nil || t.sandbox.GetType() == sandbox.SandboxTypeDisabled {
		return &types.ToolResult{Success: false, Error: "sandbox is disabled, write_file tool is unavailable"}, nil
	}

	// Resolve and validate the target path (must stay inside workDir).
	cleaned := filepath.Clean(input.Path)
	if filepath.IsAbs(cleaned) {
		return &types.ToolResult{Success: false, Error: "path must be relative"}, nil
	}
	target := filepath.Join(t.workDir, cleaned)
	rel, err := filepath.Rel(t.workDir, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return &types.ToolResult{Success: false, Error: "path escapes the session workspace"}, nil
	}

	// Create parent dirs on the host side so the target location exists inside
	// the sandbox mount. 0777 so the sandbox container user (uid 1000) can
	// write into it regardless of the host process's user.
	if err := os.MkdirAll(filepath.Dir(target), 0o777); err != nil {
		return &types.ToolResult{Success: false, Error: fmt.Sprintf("failed to create directories: %v", err)}, nil
	}

	// Generate a fixed, safe wrapper script and stream the user content via
	// stdin. The static validator inspects the wrapper (always the same) rather
	// than the arbitrary file content, while stdin validation still applies.
	scriptPath, err := t.writeWrapperScript()
	if err != nil {
		return &types.ToolResult{Success: false, Error: fmt.Sprintf("failed to prepare script: %v", err)}, nil
	}

	cfg := &sandbox.ExecuteConfig{
		Script:      scriptPath,
		Args:        []string{filepath.Base(target)},
		WorkDir:     t.workDir,
		Stdin:       input.Content,
		Timeout:     60 * time.Second,
		WritableDir: t.workDir,
	}
	result, err := t.sandbox.Execute(ctx, cfg)
	if err != nil {
		logger.Errorf(ctx, "[Tool][WriteFile] execution failed: %v", err)
		return &types.ToolResult{Success: false, Error: fmt.Sprintf("execution failed: %v", err)}, nil
	}
	if !result.IsSuccess() {
		msg := result.Error
		if msg == "" {
			msg = fmt.Sprintf("write failed with exit code %d", result.ExitCode)
		}
		return &types.ToolResult{Success: false, Error: msg, Output: result.GetOutput()}, nil
	}

	abs, _ := filepath.Abs(target)
	return &types.ToolResult{
		Success: true,
		Output:  fmt.Sprintf("File written successfully.\nPath: %s\nSize: %d bytes", abs, len(input.Content)),
		Data: map[string]interface{}{
			"path":  abs,
			"bytes": len(input.Content),
		},
	}, nil
}

// writeWrapperScript creates a safe sh wrapper that reads stdin into the target
// file. The target basename is passed as the only argument; the workdir is the
// sandbox working directory, so the wrapper writes into it.
func (t *WriteFileTool) writeWrapperScript() (string, error) {
	if err := os.MkdirAll(t.workDir, 0o777); err != nil {
		return "", err
	}
	scriptPath := filepath.Join(t.workDir, fmt.Sprintf("write_%d.sh", time.Now().UnixNano()))
	// The wrapper reads stdin and writes to the given filename in the CWD.
	content := "#!/bin/sh\ncat > \"$1\"\n"
	if err := os.WriteFile(scriptPath, []byte(content), 0o644); err != nil {
		return "", err
	}
	return scriptPath, nil
}

// Cleanup implements types.Tool.
func (t *WriteFileTool) Cleanup(ctx context.Context) error {
	return nil
}
