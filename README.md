# WhatsApp Web Clone

## Run locally

### Backend
1. Create virtualenv, install deps.
2. Set `.env` with `MONGODB_URI` and `MONGODB_NAME`.
3. Run:
```bash
uvicorn app.main:app --reload
