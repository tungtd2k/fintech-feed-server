# Fintech market feed

```powershell
cd E:\app_flutter\fintech\feed-server
python generate_data.py
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python replay_server.py --speed 10
```

The Flutter app in `app/` connects to `ws://127.0.0.1:8765`.
