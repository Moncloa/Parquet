from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from uuid import uuid4

from parquet.bridge.github import GitHubBridge
from parquet.config import Settings
from parquet.models import MarketAnalysis, ReviewRequest
from parquet.scheduler import ReviewQueue, ScheduledReview
from parquet.storage import Storage


class Orchestrator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.storage = Storage(settings.state_db)
        self.reviews = ReviewQueue()
        self.bridge = (
            GitHubBridge(
                settings.github.repository,
                settings.github.runtime_pr,
                settings.github.token_file,
            )
            if settings.github.enabled
            else None
        )

    def process_analysis(self, analysis: MarketAnalysis) -> None:
        self.storage.save_analysis(
            analysis.analysis_id,
            analysis.generated_at.isoformat(),
            analysis.model_dump_json(),
        )
        if analysis.next_review is not None:
            self.reviews.add(
                ScheduledReview(at=analysis.next_review.at, reason=analysis.next_review.reason)
            )
        self.storage.set("latest_analysis_id", analysis.analysis_id)

    async def poll_github_once(self) -> int:
        if self.bridge is None:
            return 0
        last_id = int(self.storage.get("github_last_comment_id") or 0)
        processed = 0
        comments = await self.bridge.comments()
        for comment in comments:
            comment_id = int(comment["id"])
            if comment_id <= last_id:
                continue
            body = str(comment.get("body", ""))
            analysis = self.bridge.parse_analysis_comment(body)
            if analysis is not None:
                self.process_analysis(analysis)
                processed += 1
            last_id = max(last_id, comment_id)
        self.storage.set("github_last_comment_id", str(last_id))
        return processed

    async def post_due_reviews(self) -> int:
        if self.bridge is None:
            return 0
        count = 0
        for review in self.reviews.due():
            request = ReviewRequest(
                request_id=str(uuid4()),
                requested_at=datetime.now(timezone.utc),
                reason=review.reason,
            )
            await self.bridge.post_review_request(request)
            self.storage.add_event("review_request", request.model_dump_json())
            count += 1
        return count

    async def run_forever(self) -> None:
        while True:
            try:
                await self.poll_github_once()
                await self.post_due_reviews()
            except Exception as exc:
                self.storage.add_event("orchestrator_error", json.dumps({"error": repr(exc)}))
            await asyncio.sleep(self.settings.poll_seconds)
