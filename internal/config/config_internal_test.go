package config

import "testing"

func TestMustEnvMissingExits(t *testing.T) {
	t.Setenv("WORKER_TEST_REQUIRED_ENV", "")
	previousExit := exitProcess
	t.Cleanup(func() { exitProcess = previousExit })
	exitCode := 0
	exitProcess = func(code int) { exitCode = code }

	if value := mustEnv("WORKER_TEST_REQUIRED_ENV"); value != "" {
		t.Fatalf("mustEnv() = %q, want empty value after intercepted exit", value)
	}
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}
