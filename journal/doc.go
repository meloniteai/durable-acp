// Package journal provides durable append-only JSONL journals for host sessions.
//
// Records use a small neutral user.* and agent.* vocabulary. Applications may
// store additional owner-qualified event names, such as example.state_changed;
// the package preserves those extension records without interpreting them.
package journal
