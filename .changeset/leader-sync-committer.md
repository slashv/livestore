---
'@livestore/common': patch
---

Commit leader sync state and eventlog transitions through a dedicated Effect service. Backend heads now persist in the same eventlog transaction as their event inserts, and commit receipts no longer mutate planned events.
