# sample-beam: part 02 Graph

## about

- This repo is for demonstrating how to generate Beam DAG in Python.
- It is written aside blogs described below.

## blog related

- [Blog] [Let's try: Apache Beam part 2 - draw the graph](https://bluebirz.net/posts/lets-try-apache-beam-part-2/)
- [Medium] [Let's try: Apache Beam part 2 - draw the graph](https://medium.com/@bluebirz/lets-try-apache-beam-part-2-draw-the-graph-95b3d568dba5)

## How to run

### Prerequisites

1. Require python env.
2. Require Graphviz. Can be install via Homebrew using `brew install graphviz`.

### Execute

```bash
pip install -r requirements.txt
cd src
python3 main.py --render_output='dag.png'
```
