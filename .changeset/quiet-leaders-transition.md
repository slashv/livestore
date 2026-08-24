---
---

Internal-only: replace leader synchronization's semaphore and queue orchestration with an explicit hierarchical state
machine and serialized command mailbox. Public processor APIs and supported synchronization behavior are unchanged.
