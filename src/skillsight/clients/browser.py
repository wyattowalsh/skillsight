"""Optional browser fallback via Camoufox."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class BrowserProbeResult:
    """Network probe output."""

    urls: list[str] = field(default_factory=list)


class BrowserClient:
    """Best-effort browser helper for diagnostics and fallback."""

    def __init__(self, *, headless: bool = True) -> None:
        self.headless = headless

    async def probe(self, url: str = "https://skills.sh/") -> BrowserProbeResult:
        """Capture API routes discovered while loading a page."""

        try:
            from camoufox.async_api import AsyncCamoufox
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("camoufox is not installed. Install extras: skillsight[browser].") from exc

        captured: set[str] = set()
        async with AsyncCamoufox(headless=self.headless) as browser:
            page = await browser.new_page()

            async def handle_response(response):  # type: ignore[no-untyped-def]
                endpoint = response.url
                if "/api/" in endpoint:
                    captured.add(endpoint)

            page.on("response", handle_response)
            await page.goto(url)
            await page.wait_for_load_state("networkidle")

        return BrowserProbeResult(urls=sorted(captured))
