import apache_beam as beam
import google.auth
from google.cloud.firestore import Client, CollectionReference


class FireStoreWriterToCollection(beam.DoFn):
    """
    Write an element into specific Firestore collection. Requires PCollection in format:
    .. code-block:: json
        {
            "key": <document key>,
            "value": <document payload>
        }

    Args:
        target_collection_path_str (str): collection path e.g. `continent/europe/country`
        database (str, optional): database name. Defaults to "(default)".
        max_elements_per_batch (int, optional): max element to process in batch. Defaults to 10.
    """

    def __init__(
        self,
        target_collection_path_str: str,
        database="(default)",
        max_elements_per_batch=500,
    ):
        self.database = database
        self.target_collection_path_str = target_collection_path_str
        self.MAX_ELEM_PER_BATCH = max_elements_per_batch

    def setup(self):
        credentials, project_id = google.auth.default()
        self.client = Client(
            project=project_id, credentials=credentials, database=self.database
        )
        self.target_collection: CollectionReference = self.client.collection(
            self.target_collection_path_str
        )

    def start_bundle(self):
        self.current_batch = []

    def finish_bundle(self):
        if self.current_batch:
            self.commit_batch()

    def process(self, element):
        self.current_batch.append(element)
        if len(self.current_batch) >= self.MAX_ELEM_PER_BATCH:
            self.commit_batch()

    def commit_batch(self):
        batch = self.client.batch()
        for elem in self.current_batch:
            document_ref = self.target_collection.document(elem.get("key"))
            print(f"Batch adding document at {document_ref.path}")
            batch.set(document_ref, elem.get("value"))

        batch.commit()
        self.current_batch = []
