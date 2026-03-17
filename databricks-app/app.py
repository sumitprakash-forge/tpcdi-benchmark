"""TPC-DI Benchmark App - FastAPI entry point."""
import os
import uvicorn
from pathlib import Path
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from server.routes import router
from server.dataproc_routes import router as dataproc_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Restore any Dataproc runs that were active before a restart
    try:
        from server.dataproc_routes import restore_active_dataproc_runs
        await restore_active_dataproc_runs()
    except Exception as e:
        print(f"[startup] WARNING: failed to restore Dataproc runs: {e}")
    yield

app = FastAPI(title="TPC-DI Benchmark", lifespan=lifespan)
app.include_router(router, prefix="/api")
app.include_router(dataproc_router, prefix="/api")

# Serve React frontend
frontend_dist = Path(__file__).parent / "frontend" / "dist"
if frontend_dist.exists():
    app.mount("/assets", StaticFiles(directory=frontend_dist / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        if full_path.startswith("api/"):
            return {"error": "Not found"}, 404
        file_path = frontend_dist / full_path
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(frontend_dist / "index.html")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
