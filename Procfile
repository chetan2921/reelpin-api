web: uvicorn app.main:app --host 0.0.0.0 --port $PORT
worker: dramatiq app.tasks --processes 4 --threads 2
