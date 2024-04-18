import apache_beam as beam
import argparse
from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference


def run_beam():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_query_file", type=str)
    parser.add_argument("--output_dataset", type=str)
    parser.add_argument("--output_table", type=str)
    parser.add_argument("--project", type=str)
    parser.add_argument("--temp_datasetId", type=str)  # same region as source query
    args, sys_args = parser.parse_known_args()
    with open(args.input_query_file, "r") as fptr:
        query = fptr.read()
    options = beam.options.pipeline_options.PipelineOptions()
    with beam.Pipeline(options=options) as pipe:
        (
            pipe
            | "Read"
            >> beam.io.ReadFromBigQuery(
                project=args.project,
                query=query,
                use_standard_sql=True,
                temp_dataset=DatasetReference(
                    # avoid auto temp dataset creation from apache beam
                    projectId=args.project,
                    datasetId=args.temp_datasetId,
                ),
            )
            | "Write"
            >> beam.io.WriteToBigQuery(
                table=args.output_table,
                dataset=args.output_dataset,
                project=args.project,
                schema="SCHEMA_AUTODETECT",
            )
        )


if __name__ == "__main__":
    run_beam()
