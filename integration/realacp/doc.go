// Package realacp contains opt-in black-box coverage for actual ACP agents.
//
// The tests require the realacp build tag and deliberately live outside the
// normal unit-test path because they install and invoke external coding
// agents. scripts/run-realacp.sh prepares their isolated environment.
package realacp
