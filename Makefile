PY := .venv/bin/python
PORT_API ?= 8000
PORT_RECEIVER ?= 9000

.PHONY: venv api receiver load test lint db db-down

venv:                      ## create .venv and install everything
	python3 -m venv .venv
	$(PY) -m pip install -q -U pip
	$(PY) -m pip install -q -e ".[dev]"

api:                       ## run the service (your code) with reload
	$(PY) -m uvicorn hookline.main:app --reload --port $(PORT_API)

receiver:                  ## run the flaky receiver
	$(PY) -m uvicorn tools.receiver.app:app --port $(PORT_RECEIVER)

load:                      ## run the load script, e.g. make load ARGS="--events 200"
	$(PY) -m tools.load $(ARGS)

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check . && $(PY) -m ruff format --check .

db:                        ## stage 2+: start postgres
	docker compose up -d postgres

db-down:
	docker compose down
