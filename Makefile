all: clean install test

api:
	LAKEHOUSE_API_AUTH_ENABLED=0 DEBUG=1 granian --interface asgi --reload --port 5000 ftm_lakehouse.api:app

start:
	docker compose up --build -d --wait

stop:
	docker compose down --remove-orphans -v


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

test: start
	@echo "── run 1: local + api variants (pytest_env LAKEHOUSE_URI) ──" ; \
	poetry run pytest -v --capture=sys --cov=ftm_lakehouse --cov-report lcov \
		-k "not docker" ; \
	rc1=$$? ; \
	echo "── run 2: docker variants (LAKEHOUSE_URI=$${LAKEHOUSE_TEST_URL:-http://127.0.0.1:8000}) ──" ; \
	LAKEHOUSE_TEST_MODE=docker \
	LAKEHOUSE_URI=$${LAKEHOUSE_TEST_URL:-http://127.0.0.1:8000} \
		poetry run pytest -v --capture=sys --cov-append --cov=ftm_lakehouse --cov-report lcov \
			-k "docker" ; \
	rc2=$$? ; \
	$(MAKE) stop ; \
	test $$rc1 -eq 0 -a $$rc2 -eq 0

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
	zensical build
	aws --profile nbg1 --endpoint-url https://s3.investigativedata.org s3 sync ./site s3://openaleph.org/docs/lib/ftm-lakehouse
