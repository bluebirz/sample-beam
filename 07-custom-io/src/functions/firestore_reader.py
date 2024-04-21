from apache_beam.io import iobase, OffsetRangeTracker
from apache_beam.metrics import Metrics
import google.auth
from google.cloud.firestore import Client, CollectionReference, DocumentSnapshot


class FireStoreReaderFromCollection(iobase.BoundedSource):
    """
    Retrieve all elements under specific Firestore collection. Return PCollection in format:
    .. code-block:: json
        {
            "key": <document key>,
            "value": <document payload>
        }

    Args:
        target_collection_path_str (str): collection path e.g. `continent/europe/country`
        database (str, optional): database name. Defaults to "(default)".
        limit (int, optional): max number of documents read. Defaults to 10_000.
    """

    def __init__(
        self,
        target_collection_path_str: str,
        database: str = "(default)",
        limit: int = 10_000,
    ):
        self.database = database
        self.target_collection_path_str = target_collection_path_str
        self.records_read = Metrics.counter(self.__class__, "recordsRead")
        self._count = limit

    def estimate_size(self):
        return self._count

    def get_range_tracker(self, start_position, stop_position):
        if start_position is None:
            start_position = 0
        if stop_position is None:
            stop_position = self._count

        return OffsetRangeTracker(start_position, stop_position)

    def read(self, range_tracker):
        credentials, project_id = google.auth.default()
        self.client: Client = Client(
            project=project_id, credentials=credentials, database=self.database
        )
        self.target_collection: CollectionReference = self.client.collection(
            self.target_collection_path_str
        )
        docs = self.target_collection.stream()
        index: int
        doc: DocumentSnapshot
        for index, doc in enumerate(docs):
            if not range_tracker.try_claim(index):
                return
            self.records_read.inc()
            yield {"key": doc.id, "value": doc.to_dict()}

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
