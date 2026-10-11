PY := .venv/bin/python
PORT_API ?= 8000
PORT_RECEIVER ?= 9000

.PHONY: venv api receiver worker load test lint db db-down db-local db-local-down db-migrate psql

venv:                      ## create .venv and install everything
	python3 -m venv .venv
	$(PY) -m pip install -q -U pip
	$(PY) -m pip install -q -e ".[dev]"

api:                       ## run the service (your code) with reload
	$(PY) -m uvicorn hookline.main:app --reload --port $(PORT_API)

worker:                    ## run one delivery worker (stage 2+); run it twice for two workers
	$(PY) -m hookline.worker

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

PG := /opt/homebrew/opt/postgresql@17/bin
PGDATA := .pgdata
export LC_ALL := C   # macOS: without it the postmaster "became multithreaded during startup"

db-local:                  ## stage 2+: start postgres from `brew install postgresql@17`, data in .pgdata/
	@test -d $(PGDATA) || ( $(PG)/initdb -U hookline --auth=trust $(PGDATA) >/dev/null \
		&& $(PG)/pg_ctl -D $(PGDATA) -l $(PGDATA)/log -o "-p 5432" start -w -s \
		&& $(PG)/createdb -U hookline hookline \
		&& $(PG)/pg_ctl -D $(PGDATA) stop -s )
	$(PG)/pg_ctl -D $(PGDATA) -l $(PGDATA)/log -o "-p 5432" start -w -s

db-local-down:
	$(PG)/pg_ctl -D $(PGDATA) stop -s

db-migrate:                ## apply src/hookline/migrations/*.sql
	$(PY) -m hookline.db migrate

psql:
	$(PG)/psql -U hookline hookline
