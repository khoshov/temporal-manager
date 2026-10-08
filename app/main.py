import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.temporal_service import temporal_service
from app.routes import api, views

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("temporal-manager")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Try connecting on startup in background (non-blocking so web UI starts immediately)
    logger.info("Starting Temporal Manager...")
    try:
        await temporal_service.connect()
    except Exception as e:
        logger.warning(f"Initial connection to Temporal was not established: {e}. You can configure it via Web UI.")
    yield
    logger.info("Shutting down Temporal Manager...")


app = FastAPI(
    title="Temporal Manager",
    description="Web dashboard for monitoring and triggering Temporal workflows",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(views.router)
app.include_router(api.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
