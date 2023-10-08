python3 -m main \
    --region europe-west1 \
    --runner DataflowRunner \
    --project bluebirz-playground \
    --temp_location gs://bluebirz-beam-dataflow-temp/

gcloud artifacts repositories create sample-beam \
   --repository-format=docker \
   --location=europe-west1 \
   --async

gcloud builds submit --tag REGION-docker.pkg.dev/PROJECT_ID/REPOSITORY/dataflow/FILE_NAME:TAG .
gcloud builds submit --tag europe-west1-docker.pkg.dev/bluebirz-playground/sample-beam/dataflow/04-dataflow .

python -m main \
  --project=bluebirz-playground \
  --region=europe-west1 \
  --temp_location=gs://bluebirz-beam-dataflow-temp/ \
  --runner=DataflowRunner \
  --experiments=use_runner_v2 \
  --sdk_container_image=europe-west1-docker.pkg.dev/bluebirz-playground/sample-beam/dataflow/04-dataflow:latest

python -m main \
  --project=bluebirz-playground \
  --region=europe-west1 \
  --input_file="gs://bluebirz-beam/dataflow-1/mock.csv"
  --output_file="gs://bluebirz-beam/dataflow-1/processed-2.csv"
  --temp_location=gs://bluebirz-beam-dataflow-temp/ \
  --runner=DataflowRunner \
  --experiments=use_runner_v2 \
  --sdk_container_image=europe-west1-docker.pkg.dev/bluebirz-playground/sample-beam/dataflow/04-dataflow:latest
