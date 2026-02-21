.PHONY: install test lint run

install:
	pip install -e src/

test:
	python -m pytest tests/ -v

lint:
	python -m py_compile src/crypto_pipeline/cli.py
	python -m py_compile src/crypto_pipeline/api_client.py
	python -m py_compile src/crypto_pipeline/metrics.py
	python -m py_compile src/crypto_pipeline/storage.py
	python -m py_compile src/crypto_pipeline/pipeline.py
	python -m py_compile src/crypto_pipeline/validation.py

run:
	crypto-pipeline --help
