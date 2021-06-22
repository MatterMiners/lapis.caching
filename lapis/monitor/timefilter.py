import logging

from usim import time
from usim._core.loop import __LOOP_STATE__


class SimulationTimeFilter(logging.Filter):
    """
    Dummy filter to replace log record timestamp with simulation time.
    """

    def filter(self, record) -> bool:
        # record.created = time.now
        record.created = time.now + (1e-9 * __LOOP_STATE__.loop.turn)
        return True
