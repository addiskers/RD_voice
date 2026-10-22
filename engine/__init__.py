"""GlobalVox Voice Engine.

Two deployables share this package:
- ``engine.api``    - REST API + webhooks + carrier callbacks (control plane of the engine).
- ``engine.worker`` - per-call conversation runtime (carrier/browser WebSocket -> LiveKit Agents session).
"""

__version__ = "0.1.0"
