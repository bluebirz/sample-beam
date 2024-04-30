import apache_beam as beam
import csv
from io import StringIO


class DictToCsvFn(beam.DoFn):
    def __init__(self, header_columns: list[str]):
        self.header_columns = header_columns

    def process(self, element):
        buffer = StringIO()
        writer = csv.DictWriter(
            buffer,
            fieldnames=self.header_columns,
            lineterminator="",
            quoting=csv.QUOTE_ALL,
        )
        # writer.writeheader()
        writer.writerow(element)
        return [buffer.getvalue()]
