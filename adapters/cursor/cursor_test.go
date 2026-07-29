package cursor

import "testing"

func TestNewDeclaresCursorBackend(t *testing.T) {
	if adapter := New(); adapter == nil || adapter.Backend() != Backend {
		t.Fatalf("adapter = %#v", adapter)
	}
}
