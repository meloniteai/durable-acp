package antigravity

import "testing"

func TestNewDeclaresAntigravityBackend(t *testing.T) {
	if adapter := New(); adapter == nil || adapter.Backend() != Backend {
		t.Fatalf("adapter = %#v", adapter)
	}
}
