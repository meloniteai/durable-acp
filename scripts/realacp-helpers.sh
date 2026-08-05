#!/usr/bin/env bash

# These helpers run inside the real-ACP runner's private temporary directory.
# Keep authentication separate from the rest of a developer's Codex state so
# live tests cannot load personal configuration, plugins, skills, or sessions.

realacp_codex_source_home() {
  local configured_home="${1:-}"
  local user_home="${2:-}"
  if [ -n "$configured_home" ]; then
    printf '%s\n' "$configured_home"
  elif [ -n "$user_home" ]; then
    printf '%s/.codex\n' "$user_home"
  fi
}

realacp_configure_codex_worker() {
  local provider="$1"
  local source_home="$2"
  local worker_home="$3"
  local codex_path="$4"

  [ -n "$worker_home" ] || { echo "Codex worker home is required" >&2; return 1; }
  [ -n "$codex_path" ] || { echo "Codex executable path is required" >&2; return 1; }
  mkdir -p "$worker_home"

  # A vanilla run needs the caller's existing login but none of their other
  # Codex state. The temporary copy can be refreshed independently and is
  # scrubbed before retained test artifacts are exposed.
  if [ "$provider" = "vanilla" ] && [ -n "$source_home" ] && [ -f "$source_home/auth.json" ]; then
    cp "$source_home/auth.json" "$worker_home/auth.json"
    chmod 600 "$worker_home/auth.json"
  fi

  export CODEX_HOME="$worker_home"
  export CODEX_PATH="$codex_path"
}

realacp_scrub_codex_auth() {
  local run_root="$1"
  local auth_file
  for auth_file in "$run_root"/workers/*/codex/auth.json; do
    [ -e "$auth_file" ] || continue
    rm -f -- "$auth_file"
  done
}

realacp_configure_openrouter() {
  local model="$1"

  [ -n "$model" ] || { echo "OpenRouter model is required" >&2; return 1; }
  [ -n "${OPENROUTER_API_KEY:-}" ] || { echo "OPENROUTER_API_KEY is required" >&2; return 1; }

  export DURABLE_ACP_REAL_MODEL="$model"
  export DURABLE_ACP_REAL_CODEX_MODEL="${DURABLE_ACP_REAL_CODEX_MODEL:-$model}"
  # The official Claude ACP exposes Anthropic aliases in its session picker.
  # The environment below maps the selected alias to the OpenRouter model.
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
}
