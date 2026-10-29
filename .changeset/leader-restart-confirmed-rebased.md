---
'@livestore/common': patch
---

Keep confirmed events out of the leader's boot-time pending set when they were rebased before confirmation. Previously a restarted leader could treat the confirmed event at the backend head as pending: it then stopped syncing with a `MaterializationJournalError`, or, after a state rebuild, removed that event from its eventlog and pushed it to the backend a second time.
