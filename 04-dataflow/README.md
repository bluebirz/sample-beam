# sample-beam: part 04 dataflow

## about

- This repo is for demonstrating how to deploy a Beam pipeline and run on Google Dataflow in Python.
- It is written aside blogs described below.

## blog related

- [EN] [Let's try: Apache Beam part 4 - live on Google Dataflow](https://www.bluebirz.net/en/lets-try-apache-beam-part-4)
- [TH] [มาใช้ Apache Beam กันเถอะ – ตอนที่ 4 ได้เวลา Google Dataflow](https://www.bluebirz.net/th/lets-try-apache-beam-part-4-th)
- [Medium] [Let's try: Apache Beam part 4 - live on Google Dataflow](https://medium.com/@bluebirz/lets-try-apache-beam-part-4-live-on-google-dataflow-c89ed62e31f6)

## How to run

### Prerequisites

1. Already have Google Dataflow API enabled.
2. Require python env.
3. For container images options:
   1. Already have repo on Google Artifact Registry

   ```bash
   gcloud artifacts repositories create REPO_NAME \
   --repository-format=docker \
   --location=LOCATION \
   --async
   ```

   2. Already have container runner e.g. Docker Desktop or OrbStack installed.

### option a: deploy from local

```bash
# run Beam pipeline from src
cd 04a-local-deploy/src
python3 -m main \
    --region LOCATION \
    --runner DataflowRunner \
    --project PROJECT_ID \
    --temp_location GCS_TEMP_LOCATION
```

### option b: deploy from image

```bash
# build image to Google Artifact Registry
cd 04b-docker-deploy
gcloud builds submit --tag LOCATION-docker.pkg.dev/PROJECT_ID/REPO_NAME/IMAGE_PATH:TAG .

# run Beam pipeline from image
cd src
python -m main \
  --project=PROJECT_ID \
  --region=LOCATION \
  --temp_location=GCS_TEMP_LOCATION \
  --runner=DataflowRunner \
  --experiments=use_runner_v2 \
  --sdk_container_image=LOCATION-docker.pkg.dev/PROJECT_ID/REPO_NAME/IMAGE_PATH:TAG
```

### option c: deploy from image with parameters

```bash
# build image to Google Artifact Registry
cd 04c-docker-deploy-params
gcloud builds submit --tag LOCATION-docker.pkg.dev/PROJECT_ID/REPO_NAME/IMAGE_PATH:TAG .
# run Beam pipeline from image
cd src
python -m main \
  --project=PROJECT_ID \
  --region=LOCATION \
  --input_file="gs://INPUT_FILE_PATH" \
  --output_file="gs://OUTPUT_FILE_PATH" \
  --temp_location=GCS_TEMP_LOCATION \
  --runner=DataflowRunner \
  --experiments=use_runner_v2 \
  --sdk_container_image=LOCATION-docker.pkg.dev/PROJECT_ID/REPO_NAME/IMAGE_PATH:TAG
```
