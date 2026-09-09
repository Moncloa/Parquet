from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx

from parquet.models import MarketAnalysis, ReviewRequest

ANALYSIS_MARKER = "[PARQUET:ANALYSIS]"
REQUEST_MARKER = "[PARQUET:REVIEW_REQUEST]"


class GitHubBridge:
    def __init__(
        self,
        repository: str,
        pr_number: int,
        token_file: Path,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.repository = repository
        self.pr_number = pr_number
        self.token_file = token_file
        self.transport = transport
        # Runtime PRs are append-only in normal operation. Start from page 1 after a
        # process restart so an existing cursor can catch up, then remember the last
        # populated page to avoid rescanning the whole thread on every poll.
        self._comments_page_hint = 1

    def _token(self) -> str:
        token = self.token_file.read_text(encoding="utf-8").strip()
        if not token:
            raise RuntimeError(f"Empty GitHub token file: {self.token_file}")
        return token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._token()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    async def comments(self) -> list[dict[str, Any]]:
        url = f"https://api.github.com/repos/{self.repository}/issues/{self.pr_number}/comments"
        comments: list[dict[str, Any]] = []
        page = max(1, self._comments_page_hint)
        last_nonempty_page = max(1, page - 1)

        async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
            while True:
                response = await client.get(
                    url,
                    headers=self._headers(),
                    params={"per_page": "100", "page": str(page)},
                )
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, list):
                    raise RuntimeError("Unexpected GitHub comments response")

                if not data:
                    # A page can disappear after deletions; move the hint back to the
                    # last page that actually contained comments.
                    self._comments_page_hint = last_nonempty_page
                    break

                comments.extend(data)
                last_nonempty_page = page
                if len(data) < 100:
                    self._comments_page_hint = page
                    break
                page += 1

        return comments

    async def post_review_request(self, request: ReviewRequest) -> None:
        await self._post(REQUEST_MARKER, request.model_dump_json(indent=2))

    async def post_analysis(self, analysis: MarketAnalysis) -> None:
        await self._post(ANALYSIS_MARKER, analysis.model_dump_json(indent=2))

    async def _post(self, marker: str, payload: str) -> None:
        url = f"https://api.github.com/repos/{self.repository}/issues/{self.pr_number}/comments"
        body = f"{marker}\n```json\n{payload}\n```"
        async with httpx.AsyncClient(timeout=20, transport=self.transport) as client:
            response = await client.post(url, headers=self._headers(), json={"body": body})
            response.raise_for_status()

    @staticmethod
    def parse_analysis_comment(body: str) -> MarketAnalysis | None:
        if ANALYSIS_MARKER not in body:
            return None
        payload = body.split(ANALYSIS_MARKER, 1)[1].strip()
        if payload.startswith("```json"):
            payload = payload[len("```json") :]
            if payload.endswith("```"):
                payload = payload[:-3]
        return MarketAnalysis.model_validate(json.loads(payload.strip()))
