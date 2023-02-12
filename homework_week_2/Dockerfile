FROM prefecthq/prefect:2.7.7-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt  --trusted-host pypi.python.org --no-cache-dir

COPY etl_web_to_gcs_parameterized.py /opt/prefect/flows
COPY etl_gcs_to_bq_parameterized.py /opt/prefect/flows
COPY data /opt/prefect/data