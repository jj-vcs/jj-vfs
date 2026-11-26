# Agent Instructions

Use these rules when reading, editing, testing, or reviewing code in [jj-vfs](https://github.com/jj-vcs/jj-vfs).

## Version Control

* **No Co-Author or Attribution Trailers:** Commit messages carry no `Co-authored-by:` trailer and no other trailer that credits a tool, a coding agent, or a person. Every author of a commit must have signed the Google CLA, and the CLA check blocks the merge of a pull request whose commits name an author who has not, including a coding agent. Remove any such trailer that a tool adds by default; fix an existing commit with `jj describe` before pushing.
