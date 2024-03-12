from __future__ import annotations

import dataclasses
import datetime
import heapq
import logging
from typing import Callable

from app import models

logger = logging.getLogger(__name__)


@dataclasses.dataclass(slots=True, frozen=True, kw_only=True, order=True)
class EventLoopItem:
    at: datetime.datetime
    f: Callable[[models.App], None] = dataclasses.field(compare=False)


class EventLoop:
    def __init__(self):
        self.loop: list[EventLoopItem] = []
        self.app: models.App | None = None

    def register(self, app: models.App):
        self.app = app

    def add(
        self,
        f: Callable[[models.App], None],
        *,
        at: datetime.datetime | None = None,
        later: datetime.timedelta | None = None,
        soon: bool = False,
    ):
        assert self.app is not None
        assert 1 == sum(
            1 for item in [at, later, soon] if item
        ), "Provide only one time specifier"

        now = datetime.datetime.now(tz=datetime.UTC)
        sched = None
        if at:
            sched = at
        if later:
            sched = now + later
        if soon:
            sched = now

        item = EventLoopItem(at=sched, f=f)
        heapq.heappush(self.loop, item)

    def get_timeout(self) -> float | None:
        if not self.loop:
            return None
        peek = self.loop[0]
        now = datetime.datetime.now(tz=datetime.UTC)
        if now >= peek.at:
            return 0.0

        return (peek.at - now).total_seconds()

    def run_cycle(self):
        assert self.app is not None
        now = datetime.datetime.now(tz=datetime.UTC)
        while True:
            if not self.loop:
                break
            if self.loop[0].at > now:
                break

            item = heapq.heappop(self.loop)
            try:
                # TODO: measure time and report warnings on long functions
                item.f(self.app)
            except Exception:
                logger.exception("Exception in loop executing function:")
