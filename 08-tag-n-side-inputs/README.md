# sample-beam: part 08 - tag & side inputs

## about

- This repo is for demonstrating how to use `beam.pvalue.TaggedOutput` for tagging and `beam.pvalue.AsIter` for side inputs as Beam features in Python.
- It is written aside blogs described below.

## blog related

- [EN] [Let's try: Apache Beam part 8 - Tags & Side inputs](https://www.bluebirz.net/en/lets-try-apache-beam-part-8)
- [TH] [มาใช้ Apache Beam กันเถอะ – ตอนที่ 8 โพย side inputs และการติด tag](https://www.bluebirz.net/th/lets-try-apache-beam-part-8-th)
- [Medium] [Let's try: Apache Beam part 8 - Tags & Side inputs]()

## How to run

### Prerequisites

1. Require python env.

### Execute

```bash
pip install -r requirements.txt
cd src
python3 main.py
```

### Remark

- 2 ways to apply side inputs
    1. Prepare side input content within pipeline

        ```python
        with beam.Pipeline(options=options) as pipe:
            side_input_content = pipe | beam.io.ReadFromText(side_input_file)
            (
                pipe
                | ...
                | "insert side input" >> beam.ParDo(SomeFn(), beam.pvalue.AsIter(side_input_content)
                | ...
            )
        ```

        Need to transform PCollection before used or get the error:
        > apache_beam.error.SideInputError: PCollection used directly as side input argument. Specify AsIter(pcollection) or AsSingleton(pcollection) to indicate how the PCollection is to be used.

    1. Prepare side input content before pipeline

        ```python
        with open(side_input_file) as fptr:
            side_input_content = fptr.read()
        with beam.Pipeline(options=options) as pipe:
            (
                pipe
                | ...
                | "insert side input" >> beam.ParDo(SomeFn(), side_input_content)
                | ...
            )
        ```

## References

- [Side input patterns](https://beam.apache.org/documentation/patterns/side-inputs/)
- [apache_beam.pvalue module](https://beam.apache.org/releases/pydoc/2.29.0/apache_beam.pvalue.html)
