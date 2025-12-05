all: clean install test

api:
	LEAKRFC_ARCHIVE__URI=./tests/fixtures/archive DEBUG=1 uvicorn ftm_lakehouse.api:app --reload --port 5000

install:
	poetry install --with dev --all-extras

lint:
	poetry run flake8 ftm_lakehouse --count --select=E9,F63,F7,F82 --show-source --statistics
	poetry run flake8 ftm_lakehouse --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

pre-commit:
	poetry run pre-commit install
	poetry run pre-commit run -a

typecheck:
	poetry run mypy --strict ftm_lakehouse

test:
	poetry run pytest -v --capture=sys --cov=ftm_lakehouse --cov-report lcov

build:
	poetry run build

clean:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

documentation:
	mkdocs build
	aws --profile nbg1 --endpoint-url https://s3.investigativedata.org s3 sync ./site s3://openaleph.org/docs/lib/ftm-lakehouse
