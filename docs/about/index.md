---
id: about_index
title:  "About ZIO Webhooks"
---

A microlibrary for reliable and persistent webhook delivery.

Below is a state diagram for each webhook handled by a server. Note that there
aren't any initial or final states as the server doesn't manage the entire
lifecycle of webhooks and events—only the subset needed for reliable webhook
delivery.

[![](https://mermaid.ink/img/eyJjb2RlIjoic3RhdGVEaWFncmFtLXYyXG4gICAgRW5hYmxlZCAtLT4gRGlzYWJsZWQgOiBkaXNhYmxlZCBleHRlcm5hbGx5XG4gICAgRW5hYmxlZCAtLT4gRW5hYmxlZCA6IGV2ZW50IGRpc3BhdGNoIHN1Y2Nlc3NcbiAgICBEaXNhYmxlZCAtLT4gRW5hYmxlZCA6IGVuYWJsZWQgZXh0ZXJuYWxseVxuICAgIEVuYWJsZWQgLS0-IFJldHJ5aW5nIDogZXZlbnQgZGlzcGF0Y2ggZmFpbHVyZVxuICAgIFJldHJ5aW5nIC0tPiBFbmFibGVkIDogcmV0cnkgcXVldWUgZW1wdGllZFxuICAgIFJldHJ5aW5nIC0tPiBVbmF2YWlsYWJsZSA6IGNvbnRpbnVvdXMgZmFpbHVyZSBmb3IgZHVyYXRpb24gRFxuICAgIFVuYXZhaWxhYmxlIC0tPiBFbmFibGVkIDogZW5hYmxlZCBleHRlcm5hbGx5XG4gICAgVW5hdmFpbGFibGUgLS0-IERpc2FibGVkIDogZGlzYWJsZWQgZXh0ZXJuYWxseSIsIm1lcm1haWQiOnt9LCJ1cGRhdGVFZGl0b3IiOmZhbHNlfQ)](https://mermaid-js.github.io/mermaid-live-editor/#/edit/eyJjb2RlIjoic3RhdGVEaWFncmFtLXYyXG4gICAgRW5hYmxlZCAtLT4gRGlzYWJsZWQgOiBkaXNhYmxlZCBleHRlcm5hbGx5XG4gICAgRW5hYmxlZCAtLT4gRW5hYmxlZCA6IGV2ZW50IGRpc3BhdGNoIHN1Y2Nlc3NcbiAgICBEaXNhYmxlZCAtLT4gRW5hYmxlZCA6IGVuYWJsZWQgZXh0ZXJuYWxseVxuICAgIEVuYWJsZWQgLS0-IFJldHJ5aW5nIDogZXZlbnQgZGlzcGF0Y2ggZmFpbHVyZVxuICAgIFJldHJ5aW5nIC0tPiBFbmFibGVkIDogcmV0cnkgcXVldWUgZW1wdGllZFxuICAgIFJldHJ5aW5nIC0tPiBVbmF2YWlsYWJsZSA6IGNvbnRpbnVvdXMgZmFpbHVyZSBmb3IgZHVyYXRpb24gRFxuICAgIFVuYXZhaWxhYmxlIC0tPiBFbmFibGVkIDogZW5hYmxlZCBleHRlcm5hbGx5XG4gICAgVW5hdmFpbGFibGUgLS0-IERpc2FibGVkIDogZGlzYWJsZWQgZXh0ZXJuYWxseSIsIm1lcm1haWQiOnt9LCJ1cGRhdGVFZGl0b3IiOmZhbHNlfQ)
```mermaid
stateDiagram-v2
    Enabled --> Disabled : disabled externally
    Enabled --> Enabled : event dispatch success
    Disabled --> Enabled : enabled externally
    Enabled --> Retrying : event dispatch failure
    Retrying --> Enabled : retry queue emptied
    Retrying --> Unavailable : continuous failure for duration D
    Unavailable --> Enabled : enabled externally
    Unavailable --> Disabled : disabled externally
```