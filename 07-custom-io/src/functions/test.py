from apache_beam.io import iobase, OffsetRangeTracker
from apache_beam.metrics import Metrics


class CountingSource(iobase.BoundedSource):
    def __init__(self, count):
        self.records_read = Metrics.counter(self.__class__, "recordsRead")
        self._count = count

    def estimate_size(self):
        return self._count

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._count

        return OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        for i in range(range_tracker.start_position(), range_tracker.stop_position()):
            if not range_tracker.try_claim(i):
                return
            self.records_read.inc()
            yield i

    def split(self, desired_bundle_size, start_position=None, stop_position=None):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._count

        bundle_start = start_position
        while bundle_start < stop_position:
            bundle_stop = min(stop_position, bundle_start + desired_bundle_size)
            yield iobase.SourceBundle(
                weight=(bundle_stop - bundle_start),
                source=self,
                start_position=bundle_start,
                stop_position=bundle_stop,
            )
            bundle_start = bundle_stop
