FROM apache/beam_python3.11_sdk:2.48.0

COPY requirements.txt .
COPY src .

RUN pip install --no-cache-dir -U -r ./requirements.txt

ENTRYPOINT [ "/opt/apache/beam/boot" ]