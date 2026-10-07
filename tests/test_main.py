import httpx

from hookline.main import app


async def test_health():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://s") as c:
        assert (await c.get("/health")).json() == {"status": "ok"}
