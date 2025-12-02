from __future__ import annotations

from fastapi import FastAPI

from modern_backend.api.routes_instances import router as instances_router
from modern_backend.api.routes_volumes import router as volumes_router
from modern_backend.api.routes_events import router as events_router
from modern_backend.api.routes_overview import router as overview_router

app = FastAPI(
    title="Modern EC2 Manager",
    version="0.1.0",
    description="FastAPI backend (async) với Clean-ish Architecture và Mock/AWS adapter.",
)

app.include_router(instances_router)
app.include_router(volumes_router)
app.include_router(events_router)
app.include_router(overview_router)


@app.get("/healthz")
async def healthcheck() -> dict:
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("modern_backend.main:app", host="0.0.0.0", port=8001, reload=True)


