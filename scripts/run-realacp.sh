#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(dirname "$script_dir")"
provider="openrouter"
agents="codex,claude"
journeys="${DURABLE_ACP_REAL_JOURNEYS:-managed,existing,queued,interrupt,permission}"

usage() {
  cat <<'EOF'
Usage: scripts/run-realacp.sh [--provider openrouter|vanilla] [--agents codex,claude,cursor,antigravity|all] [--journeys managed,existing,queued,interrupt,permission|all]

The runner installs the selected public Codex/Claude ACPs and coding CLIs once,
installs Cursor CLI into its temporary home when needed, and builds the pinned
Antigravity ACP bridge plus agy when needed. It compiles the realacp Go test
binary once, then runs each selected agent journey in a separate state directory.

Journeys: managed (worktree, journal, restart/resume, cleanup), existing
(caller-owned workspace plus attachment), queued (serial turn queue), interrupt
(cancel + recovery), and permission (standard ACP permission callback).
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --provider)
      [ "$#" -ge 2 ] || { echo "--provider requires openrouter or vanilla" >&2; exit 2; }
      provider="$2"
      shift 2
      ;;
    --agents)
      [ "$#" -ge 2 ] || { echo "--agents requires a comma-separated list or all" >&2; exit 2; }
      agents="$2"
      shift 2
      ;;
    --journeys)
      [ "$#" -ge 2 ] || { echo "--journeys requires a comma-separated list or all" >&2; exit 2; }
      journeys="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$provider" in
  openrouter|vanilla) ;;
  *) echo "unsupported provider: $provider" >&2; exit 2 ;;
esac

if [ "$agents" = "all" ]; then
  agents="codex,claude,cursor,antigravity"
fi

if [ "$journeys" = "all" ]; then
  journeys="managed,existing,queued,interrupt,permission"
fi

case ",$agents," in
  *",,"*|",") echo "--agents must not be empty" >&2; exit 2 ;;
esac
for agent in ${agents//,/ }; do
  case "$agent" in
    codex|claude|cursor|antigravity) ;;
    *) echo "unsupported agent: $agent" >&2; exit 2 ;;
  esac
done

case ",$journeys," in
  *",,"*|",") echo "--journeys must not be empty" >&2; exit 2 ;;
esac
for journey in ${journeys//,/ }; do
  case "$journey" in
    managed|existing|queued|interrupt|permission) ;;
    *) echo "unsupported journey: $journey" >&2; exit 2 ;;
  esac
done

if [ "$provider" = "openrouter" ]; then
  case "${OPENROUTER_API_KEY:-}" in
    ""|*THIS\ NEEDS\ TO\ BE\ PROVIDED*)
      echo "OPENROUTER_API_KEY is required for --provider openrouter" >&2
      exit 1
      ;;
  esac
fi

for command in git go node npm; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done

contains_agent() {
  case ",$agents," in
    *",$1,"*) return 0 ;;
    *) return 1 ;;
  esac
}

if contains_agent cursor; then
  command -v curl >/dev/null || { echo "curl is required to install Cursor CLI" >&2; exit 1; }
fi

run_root="$(mktemp -d "${TMPDIR:-/tmp}/durable-acp-realacp.XXXXXX")"
pids=()
worker_names=()
worker_logs=()
worker_agents=()
worker_results=()
cleanup() {
  local pid
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  if [ "${DURABLE_ACP_REAL_KEEP_ARTIFACTS:-}" = "1" ]; then
    printf 'Real ACP artifacts kept at %s\n' "$run_root"
  else
    rm -rf "$run_root"
  fi
}
trap cleanup EXIT

deps="$run_root/deps"
mkdir -p "$deps"
packages=()
if contains_agent codex; then
  packages+=("@agentclientprotocol/codex-acp@1.1.7" "@openai/codex@0.145.0-alpha.4")
fi
if contains_agent claude; then
  packages+=("@agentclientprotocol/claude-agent-acp@0.55.0" "@anthropic-ai/claude-code@2.1.220")
fi
if [ "${#packages[@]}" -gt 0 ]; then
  npm install --prefix "$deps" --silent --no-audit --no-fund --omit=dev "${packages[@]}"
fi

if contains_agent codex; then
  export DURABLE_ACP_REAL_CODEX_ACP="$deps/node_modules/.bin/codex-acp"
  export DURABLE_ACP_REAL_CODEX_CLI="$deps/node_modules/.bin/codex"
  for binary in "$DURABLE_ACP_REAL_CODEX_ACP" "$DURABLE_ACP_REAL_CODEX_CLI"; do
    [ -x "$binary" ] || { echo "installed executable is missing: $binary" >&2; exit 1; }
  done
  "$DURABLE_ACP_REAL_CODEX_CLI" --version >/dev/null
fi

if contains_agent claude; then
  export DURABLE_ACP_REAL_CLAUDE_ACP="$deps/node_modules/.bin/claude-agent-acp"
  export DURABLE_ACP_REAL_CLAUDE_CLI="$deps/node_modules/.bin/claude"
  export CLAUDE_CODE_EXECUTABLE="$DURABLE_ACP_REAL_CLAUDE_CLI"
  for binary in "$DURABLE_ACP_REAL_CLAUDE_ACP" "$DURABLE_ACP_REAL_CLAUDE_CLI"; do
    [ -x "$binary" ] || { echo "installed executable is missing: $binary" >&2; exit 1; }
  done
  "$DURABLE_ACP_REAL_CLAUDE_CLI" --version >/dev/null
fi

if contains_agent cursor && [ -z "${DURABLE_ACP_REAL_CURSOR_ACP:-}" ]; then
  if command -v agent >/dev/null; then
    export DURABLE_ACP_REAL_CURSOR_ACP="$(command -v agent)"
    export DURABLE_ACP_REAL_CURSOR_ACP_ARGS="${DURABLE_ACP_REAL_CURSOR_ACP_ARGS:-acp}"
  elif command -v cursor-agent >/dev/null; then
    export DURABLE_ACP_REAL_CURSOR_ACP="$(command -v cursor-agent)"
    export DURABLE_ACP_REAL_CURSOR_ACP_ARGS="${DURABLE_ACP_REAL_CURSOR_ACP_ARGS:-acp}"
  else
    cursor_home="$run_root/cursor-home"
    mkdir -p "$cursor_home"
    HOME="$cursor_home" curl https://cursor.com/install -fsS | HOME="$cursor_home" bash
    cursor_binary="$cursor_home/.local/bin/cursor-agent"
    [ -x "$cursor_binary" ] || { echo "Cursor installation did not provide cursor-agent" >&2; exit 1; }
    export DURABLE_ACP_REAL_CURSOR_ACP="$cursor_binary"
    export DURABLE_ACP_REAL_CURSOR_ACP_ARGS="${DURABLE_ACP_REAL_CURSOR_ACP_ARGS:-acp}"
  fi
fi

if contains_agent antigravity && [ -z "${DURABLE_ACP_REAL_ANTIGRAVITY_ACP:-}" ]; then
  antigravity_repo="${DURABLE_ACP_REAL_ANTIGRAVITY_ACP_REPOSITORY:-https://github.com/meloniteai/antigravity-acp-go.git}"
  # This concrete provider dependency stays outside the durable-acp module.
  antigravity_ref="${DURABLE_ACP_REAL_ANTIGRAVITY_ACP_REF:-c2b549a051777143757450ab43a995e0914e1852}"
  antigravity_source="$deps/antigravity-acp-go"
  antigravity_binary="$deps/bin/antigravity-acp"
  git clone --quiet "$antigravity_repo" "$antigravity_source"
  git -C "$antigravity_source" checkout --quiet "$antigravity_ref"
  mkdir -p "$(dirname "$antigravity_binary")" "$run_root/antigravity-state"
  go -C "$antigravity_source" build -o "$antigravity_binary" ./cmd/antigravity-acp
  export DURABLE_ACP_REAL_ANTIGRAVITY_ACP="$antigravity_binary"
  antigravity_built_by_runner=1
fi

if [ "$provider" = "openrouter" ]; then
  model="${DURABLE_ACP_REAL_MODEL:-deepseek/deepseek-v4-flash}"
  export DURABLE_ACP_REAL_MODEL="$model"
  export DURABLE_ACP_REAL_CODEX_MODEL="${DURABLE_ACP_REAL_CODEX_MODEL:-$model}"
  # Claude Code's ACP model picker uses aliases; ANTHROPIC_MODEL above maps
  # the default `opus` alias to the OpenRouter model.
  export DURABLE_ACP_REAL_CLAUDE_MODEL="${DURABLE_ACP_REAL_CLAUDE_MODEL:-opus}"
  export DURABLE_ACP_REAL_REASONING="${DURABLE_ACP_REAL_REASONING:-low}"
  export ANTHROPIC_BASE_URL="${DURABLE_ACP_CLAUDE_OPENROUTER_BASE_URL:-https://openrouter.ai/api}"
  export ANTHROPIC_AUTH_TOKEN="$OPENROUTER_API_KEY"
  export ANTHROPIC_API_KEY=""
  export ANTHROPIC_MODEL="$model"
  export ANTHROPIC_DEFAULT_OPUS_MODEL="$model"
  export ANTHROPIC_DEFAULT_SONNET_MODEL="$model"
  export ANTHROPIC_DEFAULT_HAIKU_MODEL="$model"
  export CLAUDE_MODEL_CONFIG="{\"availableModels\":[\"$model\"]}"
  export CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
fi

if contains_agent antigravity; then
  # agy has its own catalog; OpenRouter model aliases are not valid here.
  export DURABLE_ACP_REAL_ANTIGRAVITY_MODEL="${DURABLE_ACP_REAL_ANTIGRAVITY_MODEL:-gemini-3.6-flash-low}"
fi

tests=""
# Interleave providers so the scheduler can run one long journey per provider
# concurrently. Individual providers remain single-flight below because some
# CLIs keep a global conversation database outside the runner's state dir.
for journey in ${journeys//,/ }; do
  for agent in ${agents//,/ }; do
    case "$journey" in
      managed) journey_name="ManagedLifecycle" ;;
      existing) journey_name="ExistingWorkspaceAttachment" ;;
      queued) journey_name="QueuedTurns" ;;
      interrupt) journey_name="InterruptAndRecovery" ;;
      permission)
        if [ "$agent" = "antigravity" ]; then
          journey_name="PermissionMode"
        else
          journey_name="PermissionRoundTrip"
        fi
        ;;
    esac
    case "$agent" in
      codex) agent_name="Codex" ;;
      claude) agent_name="Claude" ;;
      cursor) agent_name="Cursor" ;;
      antigravity) agent_name="Antigravity" ;;
    esac
    name="TestRealACP${agent_name}${journey_name}"
    if [ -n "$tests" ]; then
      tests="$tests|"
    fi
    tests="$tests$name"
  done
done

test_binary="$run_root/realacp.test"
cd "$repo_root"
go test -c -tags=realacp -o "$test_binary" ./integration/realacp

test_results_dir="${DURABLE_ACP_REAL_TEST_RESULTS_DIR:-}"
if [ -n "$test_results_dir" ]; then
  case "$test_results_dir" in
    /*) ;;
    *) test_results_dir="$repo_root/$test_results_dir" ;;
  esac
  mkdir -p "$test_results_dir"
fi

test_names=()
while IFS= read -r test_name; do
  [ -n "$test_name" ] && test_names+=("$test_name")
done < <("$test_binary" -test.list "^($tests)$")
if [ "${#test_names[@]}" -eq 0 ]; then
  echo "no real ACP tests matched: $tests" >&2
  exit 1
fi

parallelism="${DURABLE_ACP_REAL_JOBS:-4}"
case "$parallelism" in
  ''|*[!0-9]*|0) echo "DURABLE_ACP_REAL_JOBS must be a positive integer" >&2; exit 2 ;;
esac
if [ "$parallelism" -gt "${#test_names[@]}" ]; then
  parallelism="${#test_names[@]}"
fi

finish_worker() {
  local index="$1"
  local pid="${pids[$index]}"
  local name="${worker_names[$index]}"
  local log="${worker_logs[$index]}"
  local test_result="${worker_results[$index]}"
  local result=0
  if wait "$pid"; then
    result=0
  else
    result=$?
  fi
  printf '\n===== %s =====\n' "$name"
  cat "$log" || result=1
  if [ -n "$test_result" ] && ! go tool test2json -t -p github.com/meloniteai/durable-acp/integration/realacp < "$log" > "$test_result"; then
    echo "failed to encode Go test result for $name" >&2
    result=1
  fi
  if ! grep -Fq -- "--- PASS: $name" "$log"; then
    echo "real ACP worker did not report a passing test result" >&2
    result=1
  fi
  if [ "$result" -ne 0 ]; then
    failures=$((failures + 1))
  fi
  unset 'pids[index]' 'worker_names[index]' 'worker_logs[index]' 'worker_agents[index]' 'worker_results[index]'
  active=$((active - 1))
}

agent_for_test() {
  case "$1" in
    TestRealACPCodex*) echo codex ;;
    TestRealACPClaude*) echo claude ;;
    TestRealACPCursor*) echo cursor ;;
    TestRealACPAntigravity*) echo antigravity ;;
    *) return 1 ;;
  esac
}

agent_is_active() {
  local wanted="$1"
  local index
  for index in "${!worker_agents[@]}"; do
    if [ "${worker_agents[$index]}" = "$wanted" ]; then
      return 0
    fi
  done
  return 1
}

finish_next_worker() {
  local index
  while :; do
    for index in "${!pids[@]}"; do
      if ! kill -0 "${pids[$index]}" 2>/dev/null; then
        finish_worker "$index"
        return
      fi
    done
    sleep 0.1
  done
}

printf 'Running %d real ACP journeys across %d agent(s) with %d concurrent workers\n' "${#test_names[@]}" "$(awk -F, '{ print NF }' <<<"$agents")" "$parallelism"
active=0
failures=0
for index in "${!test_names[@]}"; do
  while [ "$active" -ge "$parallelism" ]; do
    finish_next_worker
  done

  name="${test_names[$index]}"
  agent="$(agent_for_test "$name")" || { echo "unknown real ACP test: $name" >&2; exit 1; }
  while agent_is_active "$agent"; do
    finish_next_worker
  done
  worker_dir="$run_root/workers/$index"
  worker_log="$worker_dir/output.log"
  worker_result=""
  if [ -n "$test_results_dir" ]; then
    worker_result="$test_results_dir/$name.json"
  fi
  worker_codex_home="$worker_dir/codex"
  worker_home="$worker_dir/home"
  mkdir -p "$worker_codex_home" "$worker_home"
  if [ "$provider" = "openrouter" ]; then
    sed \
      -e "s|__MODEL__|$DURABLE_ACP_REAL_MODEL|g" \
      -e "s|__BASE_URL__|${DURABLE_ACP_OPENROUTER_BASE_URL:-https://openrouter.ai/api/v1}|g" \
      "$script_dir/realacp-openrouter.codex.toml" > "$worker_codex_home/config.toml"
  fi
  (
    cd "$repo_root"
    export CODEX_HOME="$worker_codex_home"
	if [ "${antigravity_built_by_runner:-}" = "1" ] && [[ "$name" = TestRealACPAntigravity* ]]; then
	  # The bridge owns a persistent session store, so every parallel test gets
	  # one instead of racing through a shared sidecar state directory.
	  export DURABLE_ACP_REAL_ANTIGRAVITY_ACP_ARGS="--state-dir $worker_dir/antigravity-state"
	fi
    # Avoid loading a developer's installed skills, plugins, credentials, or
    # telemetry configuration. OpenRouter supplies Codex/Claude auth, but
    # Cursor and agy keep their normal provider credentials in their homes.
    if [ "$provider" = "openrouter" ] && { [[ "$name" = TestRealACPCodex* ]] || [[ "$name" = TestRealACPClaude* ]]; }; then
      export HOME="$worker_home"
      export CLAUDE_CONFIG_DIR="$worker_home/.claude"
      export XDG_CONFIG_HOME="$worker_home/.config"
    fi
    "$test_binary" -test.v -test.count=1 -test.timeout=8m -test.run "^$name$"
  ) >"$worker_log" 2>&1 &
  pids[index]=$!
  worker_names[index]="$name"
  worker_logs[index]="$worker_log"
  worker_agents[index]="$agent"
  worker_results[index]="$worker_result"
  active=$((active + 1))
done

while [ "$active" -gt 0 ]; do
  finish_next_worker
done

if [ "$failures" -ne 0 ]; then
  echo "$failures real ACP worker(s) failed" >&2
  exit 1
fi
