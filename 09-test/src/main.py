import apache_beam as beam
from transform.sample_transform import sample_transform


def run_beam():
    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipe:
        pipe | beam.Create([1, 2, 3, 4]) | beam.Map(print)


if __name__ == "__main__":
    run_beam()
