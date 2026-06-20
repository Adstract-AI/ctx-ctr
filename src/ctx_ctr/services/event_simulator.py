"""CTR event simulation service."""

from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from ctx_ctr.logging_config import get_logger
from ctx_ctr.models.events import CtrEvent, EventBatch
from ctx_ctr.services.ctr_math import logit, sigmoid
from ctx_ctr.services.seed_values_dataset import (
    AD_CATEGORIES,
    AD_WEIGHTS,
    BASELINE_CTR,
    CONTEXT_WEIGHTS,
    CONVERSATION_CATEGORIES,
    DOMAIN_WEIGHTS,
    PUBLISHER_DOMAINS,
)

logger = get_logger(__name__)


class EventPublisher(Protocol):
    """Event publishing operations required by the simulator."""

    def publish(self, event: CtrEvent, *, also_unified: bool) -> None: ...
    def flush(self) -> None: ...


class EventProducerRunConfig(BaseModel):
    """Runtime controls for simulated event production."""

    impressions: int = Field(gt=0)
    events_per_second: float = Field(ge=0)
    random_seed: int
    log_every: int = Field(ge=0)
    also_unified: bool = False
    dry_run: bool = False

    model_config = ConfigDict(frozen=True)


class EventProducerResult(BaseModel):
    """Summary of a simulated event producer run."""

    dry_run: bool
    impressions: int
    clicks: int
    total_events: int

    model_config = ConfigDict(frozen=True)


@dataclass(frozen=True)
class EventCandidate:
    """Weighted bucket candidate used by the simulator."""

    ad_category: str
    publisher_domain: str
    conversation_category: str
    traffic_weight: float


class EventSimulatorService:
    """Generate and publish realistic CTR impression and click events."""

    def __init__(self, publisher: EventPublisher) -> None:
        self._publisher = publisher

    def produce(self, config: EventProducerRunConfig) -> EventProducerResult:
        """Produce simulated CTR events according to the run configuration."""

        batch = self.build_batch(config.impressions, config.random_seed)
        if config.dry_run:
            logger.info(
                f"Prepared event production dry-run with "
                f"{batch.impression_count} impressions and {batch.click_count} clicks"
            )
            return self._result_from_batch(batch, dry_run=True)

        delay_seconds = 1.0 / config.events_per_second if config.events_per_second > 0 else 0.0
        logger.info(
            f"Starting event production: {batch.impression_count} impressions, "
            f"{batch.click_count} clicks, {len(batch.events)} total events"
        )
        for event_index, event in enumerate(batch.events, start=1):
            self._publisher.publish(event, also_unified=config.also_unified)
            if self._should_log_progress(event_index, len(batch.events), config.log_every):
                logger.info(f"Produced {event_index}/{len(batch.events)} events")
            if delay_seconds > 0:
                time.sleep(delay_seconds)
        logger.info("Flushing produced events")
        self._publisher.flush()
        logger.info(
            f"Produced {batch.impression_count} impressions and {batch.click_count} clicks"
        )
        return self._result_from_batch(batch, dry_run=False)

    def build_batch(self, impressions: int, random_seed: int) -> EventBatch:
        """Build a deterministic batch of simulated impression and click events."""

        randomizer = random.Random(random_seed)
        candidates = self._build_candidates()
        events: list[CtrEvent] = []
        base_time = datetime.now(tz=UTC)

        for index in range(impressions):
            candidate = self._choose_candidate(candidates, randomizer)
            occurred_at = base_time + timedelta(milliseconds=index * 100)
            impression_id = f"imp-{uuid.uuid5(uuid.NAMESPACE_URL, f'{random_seed}:imp:{index}')}"
            impression = CtrEvent(
                event_id=impression_id,
                event_type="impression",
                ad_category=candidate.ad_category,
                publisher_domain=candidate.publisher_domain,
                conversation_category=candidate.conversation_category,
                occurred_at=occurred_at,
            )
            events.append(impression)

            click_probability = self._click_probability(candidate)
            if randomizer.random() <= click_probability:
                click_id = f"clk-{uuid.uuid5(uuid.NAMESPACE_URL, f'{random_seed}:clk:{index}')}"
                events.append(
                    CtrEvent(
                        event_id=click_id,
                        event_type="click",
                        ad_category=candidate.ad_category,
                        publisher_domain=candidate.publisher_domain,
                        conversation_category=candidate.conversation_category,
                        occurred_at=occurred_at + timedelta(seconds=randomizer.uniform(1.0, 20.0)),
                        impression_event_id=impression_id,
                    )
                )

        return EventBatch(events=events)

    def _build_candidates(self) -> list[EventCandidate]:
        candidates: list[EventCandidate] = []
        for ad_index, ad_category in enumerate(AD_CATEGORIES):
            for domain_index, publisher_domain in enumerate(PUBLISHER_DOMAINS):
                for context_index, conversation_category in enumerate(CONVERSATION_CATEGORIES):
                    traffic_weight = (
                        1.0
                        + (len(AD_CATEGORIES) - ad_index) * 0.25
                        + (len(PUBLISHER_DOMAINS) - domain_index) * 0.2
                        + (len(CONVERSATION_CATEGORIES) - context_index) * 0.15
                    )
                    candidates.append(
                        EventCandidate(
                            ad_category=ad_category,
                            publisher_domain=publisher_domain,
                            conversation_category=conversation_category,
                            traffic_weight=traffic_weight,
                        )
                    )
        return candidates

    def _choose_candidate(
        self,
        candidates: list[EventCandidate],
        randomizer: random.Random,
    ) -> EventCandidate:
        total_weight = sum(candidate.traffic_weight for candidate in candidates)
        selected = randomizer.uniform(0, total_weight)
        running_weight = 0.0
        for candidate in candidates:
            running_weight += candidate.traffic_weight
            if running_weight >= selected:
                return candidate
        return candidates[-1]

    def _click_probability(self, candidate: EventCandidate) -> float:
        return sigmoid(
            logit(BASELINE_CTR)
            + AD_WEIGHTS[candidate.ad_category]
            + DOMAIN_WEIGHTS[candidate.publisher_domain]
            + CONTEXT_WEIGHTS[candidate.conversation_category]
        )

    def _result_from_batch(self, batch: EventBatch, *, dry_run: bool) -> EventProducerResult:
        return EventProducerResult(
            dry_run=dry_run,
            impressions=batch.impression_count,
            clicks=batch.click_count,
            total_events=len(batch.events),
        )

    def _should_log_progress(self, event_index: int, total_events: int, log_every: int) -> bool:
        if log_every == 0:
            return False
        return event_index == total_events or event_index % log_every == 0
