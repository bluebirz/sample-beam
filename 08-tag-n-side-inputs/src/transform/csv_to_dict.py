import apache_beam as beam
import csv
from typing import Callable


class CSVToDictFn(beam.DoFn):
    def __init__(self, schema: list[tuple[str, Callable]]):
        self.schema = schema

    def process(self, element):
        for l in csv.reader(
            [element],
            delimiter=",",
            quotechar='"',
            quoting=csv.QUOTE_ALL,
            skipinitialspace=True,
        ):
            result = dict()
            for z in zip(self.schema, l):
                # zip product = [ (("id", int), "1"), (("first_name", str),"Kenneth"), ... ]
                # z[0][0] --> "id"
                # z[0][1]() --> int()
                # z[1] --> "1"
                result[z[0][0]] = z[0][1](z[1])

        yield result
