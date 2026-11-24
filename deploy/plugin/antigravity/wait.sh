#!/bin/sh
# a2a waiter for agy: the doorbell.
#
# The agent starts it ONCE, as a BACKGROUND command OUTSIDE agy's sandbox
# (run_command's BypassSandbox: true; agy asks once, "always allow" keeps it):
#
#     sh ~/.gemini/config/plugins/a2a/wait.sh
#
# It keeps running. Each time new a2a mail arrives it rings the conversation
# that started it with agy's own `agentapi send-message`, which starts a turn
# in an idle conversation and joins a running one at its next step (measured,
# agy 1.5.14). Inside the sandbox agentapi cannot connect, so there it says
# so and stops.
#
# It reads only files the a2a MCP server writes in its mailbox,
# <PLUGIN_DATA>/boxes/<project hash>/:
#   live   {pid, ...}: a receiver is here while that pid lives
#   route  the conversation a2a answers in
#   mail   "N new a2a messages waiting" while anything is queued, else empty
#   bell   a counter, raised whenever new messages are queued
# and writes `waiter` (its pid, as it starts) and `beat` (every 3 s).
#
# agy gives every command ANTIGRAVITY_CONVERSATION_ID, ANTIGRAVITY_LS_ADDRESS
# and ANTIGRAVITY_CSRF_TOKEN; agentapi reads them itself. This script uses
# only the conversation id, and throws agentapi's output away.

conv=${ANTIGRAVITY_CONVERSATION_ID:-}
if [ -z "$conv" ]; then
  echo "a2a: this waiter only runs inside agy. Nothing to do."
  exit 0
fi

D=
for root in "${ANTIGRAVITY_APP_DATA_DIR:+$ANTIGRAVITY_APP_DATA_DIR/plugin_data/a2a/boxes}" \
            "$HOME/.gemini/antigravity-cli/plugin_data/a2a/boxes"; do
  [ -n "$root" ] && [ -d "$root" ] || continue
  for b in "$root"/*/; do
    if [ "$(cat "${b}route" 2>/dev/null)" = "$conv" ]; then
      D=${b%/}
      break 2
    fi
  done
done
if [ -z "$D" ]; then
  echo "a2a: no a2a mailbox answers in this conversation. Call a2a_inbox, then do what its last line says."
  exit 0
fi

# Beat at once, before the agentapi check: agy ends the turn that starts this
# right away, and its Stop hook read "no beat yet" as "no waiter" and sent the
# agent back for nothing (a real run). `waiter` is written only after the
# check, so a waiter that cannot ring never takes over one that can; until
# then its pid is in `starting`, which the hook also counts.
echo $$ > "$D/starting"
touch "$D/beat" 2>/dev/null

API=$(command -v agentapi 2>/dev/null)
[ -n "$API" ] || API=${ANTIGRAVITY_AGENTAPI_EXE:-}
if [ -z "$API" ] || ! "$API" get-conversation-metadata "$conv" >/dev/null 2>&1; then
  # Our early beat must not read as a running waiter; one that IS running
  # keeps its own.
  [ "$(cat "$D/starting" 2>/dev/null)" = "$$" ] && rm -f "$D/starting"
  w=$(cat "$D/waiter" 2>/dev/null)
  case "$w" in ''|*[!0-9]*) rm -f "$D/beat" ;;
    *) kill -0 "$w" 2>/dev/null || rm -f "$D/beat" ;; esac
  echo "a2a: the waiter cannot reach agy's agentapi, so it could wake nobody (inside agy's sandbox it cannot connect). Start it again as a BACKGROUND command with BypassSandbox: true: sh ~/.gemini/config/plugins/a2a/wait.sh"
  exit 1
fi

echo $$ > "$D/waiter"
[ "$(cat "$D/starting" 2>/dev/null)" = "$$" ] && rm -f "$D/starting"
# `beat` goes with the waiter, so nobody reads a fresh beat from one that has
# just exited as "a waiter is running".
gone() { [ "$(cat "$D/waiter" 2>/dev/null)" = "$$" ] && rm -f "$D/beat"; }
bell() {
  n=$(cat "$D/bell" 2>/dev/null)
  case "$n" in ''|*[!0-9]*) echo 0 ;; *) echo "$n" ;; esac
}
# Mail already waiting is rung at once; otherwise only what comes next.
if [ -s "$D/mail" ]; then rung=0; else rung=$(bell); fi

while :; do
  pid=$(sed -n 's/.*"pid": *\([0-9][0-9]*\).*/\1/p' "$D/live" 2>/dev/null)
  if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
    gone
    echo "a2a: this project is not receiving any more (a2a off, or agy's a2a client stopped). Do not start the waiter again."
    exit 0
  fi
  if [ "$(cat "$D/route" 2>/dev/null)" != "$conv" ]; then
    gone
    echo "a2a: a2a answers in another conversation now; this waiter stops. Nothing to do."
    exit 0
  fi
  if [ "$(cat "$D/waiter" 2>/dev/null)" != "$$" ]; then
    echo "a2a: a newer waiter took over. Nothing to do."
    exit 0
  fi
  n=$(bell)
  if [ -s "$D/mail" ] && [ "$n" -gt "$rung" ]; then
    if "$API" send-message --title "a2a" "$conv" \
        "a2a: $(cat "$D/mail"). Call a2a_inbox now." >/dev/null 2>&1; then
      rung=$n
    else
      # The ring failed: exiting wakes agy the old way.
      gone
      echo "a2a: $(cat "$D/mail"). Call a2a_inbox now."
      exit 0
    fi
  fi
  touch "$D/beat" 2>/dev/null
  sleep 3
done
