# CLAUDE.md

Guidance for Claude Code working under `Applications/`. This file is loaded
automatically at the start of every session in this tree, on any machine that
has the repo checked out.

## Read these first

- **[AGENTS.md](AGENTS.md)** — the normative coding policy for this tree
  (Win32 C-first rules, layout, build output, style, security, testing,
  commit/PR conventions). Everything in AGENTS.md applies to Claude. Follow it.
- **[README.md](README.md)** — human-facing layout and how-to.

Do not restate AGENTS.md here; defer to it.

## Working style in this repo

- **The user performs ALL git operations.** Claude never runs
  `git commit`, `git push`, `git pull`, `git fetch`, or branch/merge commands.
  Stage nothing and commit nothing. When work is ready, tell the user what
  changed and let them commit/push. (They sync between machines by hand.)
- When you do author commit messages or PR text for the user to use, follow
  the AGENTS.md Conventional Commits format, and append the attribution lines
  the session requires.

## Cross-machine session handoff

The user works on this project from two machines (a main desktop and a travel
laptop) and moves between them through git — there is no automatic session
sync. To carry in-flight context across machines:

- **[SESSION-HANDOFF.md](SESSION-HANDOFF.md)** holds the current working state:
  what is in progress, recent decisions, and the next steps. Treat it as the
  baton passed between machines.
- **At the end of a working session**, update SESSION-HANDOFF.md so it reflects
  reality, then tell the user it is ready to commit. The user commits/pushes,
  then pulls on the other machine.
- **At the start of a session**, read SESSION-HANDOFF.md and pick up from
  "Next steps."

Keep SESSION-HANDOFF.md concise and current — it is a live handoff note, not a
changelog. Git history is the permanent record.

## Current focus

Active application: **ElectionExplorer** — a Win32 (C11, no MFC) GUI voter-list
explorer. See [ElectionExplorer/](ElectionExplorer/) and
[SESSION-HANDOFF.md](SESSION-HANDOFF.md) for current state.
