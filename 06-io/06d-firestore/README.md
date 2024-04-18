# sample-beam: part 06 IO - case C : Google Cloud Pub/Sub

## about

- This repo is for demonstrating how to use Beam IO functions in Python.
- It is written aside blogs described below.
- This is part D : [Google Cloud Firestore](https://firebase.google.com/docs/firestore).

## blog related

- [EN] []()
- [TH] []()
- [Medium] []()

## Flowchart

```mermaid
sequenceDiagram
    actor beam as Apache Beam
```

## How to run

### Prerequisites

1. Require python env.
2. Install dependencies.

```shell
pip install -r requirements.txt
```

### Run locally

```shell
cd src
python3 main.py \
  --runner=DirectRunner \
  --project=PROJECT_ID
```

### Run on Google Dataflow

1. Submit an image

```shell
gcloud builds submit . \
  --tag LOCATION-docker.pkg.dev/PROJECT_ID/REPO_NAME/IMAGE_PATH:TAG
```

2. Run dataflow from the image

```shell
cd src

```
