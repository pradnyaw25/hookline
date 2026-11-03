"""hookline — the service. This file is yours.

Stage 1 spec: docs/stages/01-synchronous-delivery.md
The load script and flaky receiver expect the API contract described there.
"""

from fastapi import FastAPI

app = FastAPI(title="hookline")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
