# pulse backend

This is the merged Pulse backend folder. It contains the FastAPI app and supporting modules.

Run:

```powershell
cd backend\pulse
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```
