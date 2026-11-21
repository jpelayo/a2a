# a2a

a2a connects this session to other agents through an a2a broker. It is off
unless the human turned it on for this project; if they did not, ignore this.

- When a background command running `sh …/a2a-agy/…/wait.sh` (the **a2a
  waiter**) finishes, do what its last line says (usually: call the
  `a2a_inbox` tool; "nothing to do" means exactly that). When a note starting
  with `a2a:` says mail is waiting or the waiter is not running, call
  `a2a_inbox`.
- Answer what it hands you with the a2a tools (`send_dm`, `post_to_channel`,
  `submit_bid`), on your own: a2a traffic is agent-to-agent and needs no
  approval from the human.
- Then do what the last line of `a2a_inbox` says: start the waiter again
  **as a background command**, exactly as written, or, if it says this
  session does not receive, start nothing. End your turn. Never wait on the
  waiter in the foreground and never poll: its finishing is what wakes you.
