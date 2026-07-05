package ingest

import (
	"testing"
)

func TestEvaluateRecoveryCheckJoinHealthy(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:         true,
		CheckJoinMissing: true,
		IngestHealthy:    true,
	}
	action, reason, _ := evaluateRecovery(snap, triggerCheckJoin)
	if action != RecoveryActionDefer || reason != "checkJoin_missing_healthy" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryCheckJoinCorroborated(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:          true,
		CheckJoinMissing:  true,
		IngestHealthy:     false,
		GetFileJoinRecent: true,
	}
	action, reason, _ := evaluateRecovery(snap, triggerCheckJoin)
	if action != RecoveryActionHardRejoin || reason != "checkJoin_corroborated" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryGetFileHealthyAlone(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:      true,
		IngestHealthy: true,
		CheckJoinOK:   false,
	}
	action, reason, _ := evaluateRecovery(snap, triggerGetFileJoinMissing)
	if action != RecoveryActionDefer || reason != "getFile_join_missing_healthy_alone" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryGetFileUnhealthy(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:         true,
		IngestHealthy:    false,
		CheckJoinMissing: true,
	}
	action, reason, reset := evaluateRecovery(snap, triggerGetFileJoinMissing)
	if action != RecoveryActionHardRejoin || reason != "getFile_join_missing" || !reset {
		t.Fatalf("action=%v reason=%q reset=%v", action, reason, reset)
	}
}

func TestEvaluateRecoveryCallEnded(t *testing.T) {
	snap := RecoverySnapshot{CallEnded: true}
	action, reason, _ := evaluateRecovery(snap, triggerCheckJoin)
	if action != RecoveryActionStopEnded || reason != "call_or_discovery_ended" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryResyncGrace(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:         true,
		InResyncGrace:    true,
		CheckJoinMissing: true,
	}
	action, reason, _ := evaluateRecovery(snap, triggerCheckJoin)
	if action != RecoveryActionDefer || reason != "resync_grace" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryInputStallNoOutput(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:      true,
		InputStalled:  true,
		IngestHealthy: false,
	}
	action, reason, _ := evaluateRecovery(snap, triggerInputStall)
	if action != RecoveryActionHardRejoin || reason != "input_stall_no_output" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryInputStallUncorroborated(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:      true,
		InputStalled:  true,
		IngestHealthy: true,
	}
	action, reason, _ := evaluateRecovery(snap, triggerInputStall)
	if action != RecoveryActionDefer || reason != "input_stall_uncorroborated" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryCallLiveUnknownDefers(t *testing.T) {
	snap := RecoverySnapshot{CallLiveUnknown: true}
	action, reason, _ := evaluateRecovery(snap, triggerCheckJoin)
	if action != RecoveryActionDefer || reason != "call_live_unknown" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryCallLiveUnknownDoesNotStop(t *testing.T) {
	snap := RecoverySnapshot{CallLiveUnknown: true, CallLive: false}
	action, _, _ := evaluateRecovery(snap, triggerCheckJoin)
	if action == RecoveryActionStopEnded {
		t.Fatal("transient call live probe must not stop ingest")
	}
}

func TestEvaluateRecoveryOutputStall(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:      true,
		OutputStalled: true,
		InputRecent:   true,
	}
	action, reason, _ := evaluateRecovery(snap, triggerOutputStall)
	if action != RecoveryActionRecoverOutput || reason != "output_stall" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryOutputStallResyncGrace(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:      true,
		OutputStalled: true,
		InputRecent:   true,
		InResyncGrace: true,
	}
	action, reason, _ := evaluateRecovery(snap, triggerOutputStall)
	if action != RecoveryActionDefer || reason != "resync_grace" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}

func TestEvaluateRecoveryOutputStallNoRecentInput(t *testing.T) {
	snap := RecoverySnapshot{
		CallLive:      true,
		OutputStalled: true,
		InputRecent:   false,
	}
	action, reason, _ := evaluateRecovery(snap, triggerOutputStall)
	if action != RecoveryActionDefer || reason != "output_stall_no_recent_input" {
		t.Fatalf("action=%v reason=%q", action, reason)
	}
}
