package console

import (
	"io"
	"os/exec"
	"runtime"
)

func EnsureUTF8Console() {
	if runtime.GOOS != "windows" {
		return
	}
	cmd := exec.Command("cmd", "/c", "chcp", "65001")
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	_ = cmd.Run()
}
