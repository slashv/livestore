---
'@livestore/common': patch
'@livestore/sync-cf': patch
---

Prevent concurrent sync pushes, interrupted Cloudflare publication, and stale leader reservations from leaving multi-session stores permanently unconverged.
