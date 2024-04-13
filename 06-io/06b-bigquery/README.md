# sample-beam: part 06 IO

## about

- This repo is for demonstrating how to manage and use a Beam functions in Python.
- It is written aside blogs described below.
- There are 4 pipelines for 4 scenarios. Please feel free to playaround by enabling/disabling each scenario.

## blog related

- [EN] [Let's try Apache Beam part 5 — transform it with Beam functions](https://www.bluebirz.net/en/lets-try-apache-beam-part-5)
- [TH] [มาใช้ Apache Beam กันเถอะ – ตอนที่ 5 Beam functions จัดให้แล้ว](https://www.bluebirz.net/th/lets-try-apache-beam-part-5-th)
- [Medium] [Let's try Apache Beam part 5 — transform it with Beam functions](https://medium.com/@bluebirz/lets-try-apache-beam-part-5-transform-it-with-beam-functions-2f2558ba07ce)

## How to run

### Prerequisites

1. Require python env.

### Scenarios

1. Women in teams (method name: `run_beam_part1`)
2. How many men and women? (method name: `run_beam_part2`)
3. Numbers of occupation (method name: `run_beam_part3`)
4. The oldest in a group (method name: `run_beam_part4`)

### Execute

```bash
pip install -r requirements.txt
cd src
python3 main.py --input_file="assets/input.txt" --output_file="assets/output.txt"
```
