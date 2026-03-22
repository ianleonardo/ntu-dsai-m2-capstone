import asyncio
import logging
import warnings
from contextlib import asynccontextmanager

from fastapi import FastAPI

# Noisy but harmless when using gcloud user ADC + BigQuery without the Storage extra.
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=r".*authenticated using end user credentials from Google Cloud SDK.*",
)
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=r".*BigQuery Storage module not found.*",
)
from fastapi.middleware.cors import CORSMiddleware

from api.endpoints import (
    build_search_directory_insiders,
    build_search_directory_stocks,
    router as api_router,
    warm_default_transactions_cache,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await asyncio.gather(
            asyncio.to_thread(build_search_directory_stocks),
            asyncio.to_thread(build_search_directory_insiders),
            asyncio.to_thread(warm_default_transactions_cache),
        )
        logger.info("Search directory + default transactions page warmed at startup")
    except Exception:
        logger.exception(
            "Startup warmup failed; first API hits will populate caches (ensure dbt mart sp500_insider_transactions is built)"
        )
    yield


app = FastAPI(title="Insider Alpha Dashboard API", lifespan=lifespan)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix="/api")

@app.get("/")
async def root():
    return {"message": "Welcome to Insider Alpha Dashboard API"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
