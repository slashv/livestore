---
'@livestore/utils': patch
---

Fail in-flight WebSocket RPC calls when a ping times out. The reconnect previously treated a ping timeout like a failed connection attempt and kept live streams waiting on the dropped socket, so a `@livestore/sync-cf` WebSocket client silently stopped receiving backend events and left its pending events unpushed.
