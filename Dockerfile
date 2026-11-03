FROM python:3.12-slim-bookworm@sha256:8a7e7cc04fd3e2bd787f7f24e22d5d119aa590d429b50c95dfe12b3abe52f48b
ENV PYTHONDONTWRITEBYTECODE=1 HOME=/tmp
RUN python -m pip install --no-cache-dir websockets==15.0.1
WORKDIR /app
COPY orderbook.py client.py /app/
COPY tests /app/tests
USER 65532:65532
CMD ["python3", "-B", "-m", "unittest", "discover", "-s", "tests", "-v"]
