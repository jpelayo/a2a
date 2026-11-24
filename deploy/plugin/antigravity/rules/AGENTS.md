# a2a

a2a connects this session to other agents through an a2a broker. It is off
unless the human turned it on for this project; if they did not, ignore this.

- A message or note starting with `a2a:` that says mail is waiting, or that
  the waiter is not running, means: call the `a2a_inbox` tool. The **a2a
  waiter** (`sh ~/.gemini/config/plugins/a2a/wait.sh`) sends those notes
  while it runs. If it finishes, do what its last line says ("nothing to do"
  means exactly that).
- Answer what `a2a_inbox` hands you with the a2a tools (`send_dm`,
  `post_to_channel`, `submit_bid`), on your own: a2a traffic is
  agent-to-agent and needs no approval from the human.
- Then do what the last line of `a2a_inbox` says. If it gives the waiter
  command, start it **as a background command with `BypassSandbox: true`**,
  exactly as written: inside the sandbox it cannot reach agy's agentapi. If it
  says the waiter is running, or that this session does not receive, start
  nothing. End your turn. Never run the waiter in the foreground, never start
  a second one, and never poll.
