from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from uuid import uuid4

from parquet.bridge.github import GitHubBridge
from parquet.config import Settings
from parquet.models import (
    MarketAnalysis,
    MarketObservation,
    ReviewRequest,
    TriggerAction,
    WatchEvent,
    WatchEventType,
)
from parquet.scheduler import ReviewQueue, ScheduledReview, ensure_structural_reviews
from parquet.storage import Storage
from parquet.watch import WatchEngine


class Orchestrator:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.storage = Storage(settings.state_db)
        self.watch_engine = WatchEngine()
        self.reviews = ReviewQueue()
        for review in self.storage.pending_reviews():
            self.reviews.add(review)
        self.bridge = (
            GitHubBridge(
                settings.github.repository,
                settings.github.runtime_pr,
                settings.github.token_file,
            )
            if settings.github.enabled
            else None
        )
        self.ensure_structural_reviews()

    def add_review(self, review: ScheduledReview) -> None:
        self.reviews.add(review)
        self.storage.schedule_review(review)

    def ensure_structural_reviews(self, now: datetime | None = None) -> None:
        before = {review.key for review in self.reviews.pending()}
        ensure_structural_reviews(
            self.reviews,
            self.settings.schedule.structural_reviews,
            now,
        )
        for review in self.reviews.pending():
            if review.key not in before:
                self.storage.schedule_review(review)

    def process_analysis(self, analysis: MarketAnalysis) -> None:
        self.storage.save_analysis(
            analysis.analysis_id,
            analysis.generated_at.isoformat(),
            analysis.model_dump_json(),
        )
        for watch in analysis.watch:
            self.storage.save_watch(analysis.analysis_id, watch)
        if analysis.next_review is not None:
            self.add_review(
                ScheduledReview(
                    at=analysis.next_review.at.astimezone(UTC),
                    reason=analysis.next_review.reason,
                    source="chatgpt",
                )
            )
        self.storage.set("latest_analysis_id", analysis.analysis_id)

    def process_observation(self, observation: MarketObservation) -> list[WatchEvent]:
        events: list[WatchEvent] = []
        for watch in self.storage.active_watches(observation.observed_at):
            event = self.watch_engine.evaluate(watch, observation)
            if event is None:
                continue
            events.append(event)
            self.storage.add_event("watch_event", event.model_dump_json())
            self.storage.set_watch_status(watch.watch_id, event.event.value)
            if event.event == WatchEventType.TRIGGERED and event.action == TriggerAction.REASSESS:
                self.add_review(
                    ScheduledReview(
                        at=observation.observed_at.astimezone(UTC),
                        reason=f"watch_trigger:{watch.watch_id}",
                        source="watch",
                    )
                )
            elif event.event == WatchEventType.TRIGGERED:
                self.storage.add_event(
                    "watch_execute_deferred",
                    json.dumps({"watch_id": watch.watch_id, "reason": "execution_not_implemented"}),
                )
        return events

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

    async def post_due_reviews(self, now: datetime | None = None) -> int:
        if self.bridge is None:
            return 0
        current = now or datetime.now(UTC)
        due = self.reviews.due(current)

        active_symbols = sorted({watch.symbol for watch in self.storage.active_watches(current)})
        count = 0
        for review in due:
            request = ReviewRequest(
                request_id=str(uuid4()),
                requested_at=current,
                reason=review.reason,
                symbols=active_symbols,
            )
            await self.bridge.post_review_request(request)
            self.storage.add_event("review_request", request.model_dump_json())
            self.storage.delete_review(review)
            count += 1
        self.ensure_structural_reviews(current)
        return count

    async def run_forever(self) -> None:
        while True:
            try:
                await self.poll_github_once()
                self.ensure_structural_reviews()
                await self.post_due_reviews()
            except Exception as exc:
                self.storage.add_event("orchestrator_error", json.dumps({"error": repr(exc)}))
            await asyncio.sleep(self.settings.poll_seconds)
