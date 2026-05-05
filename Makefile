.PHONY: stress stress-staging install-loadtest help test lint

help:
	@echo "Backend make targets:"
	@echo "  test            Run pytest (464 tests)"
	@echo "  lint            Run ruff check"
	@echo "  stress          Run Locust scenario A against LOCAL backend (--workers 1)"
	@echo "  stress-staging  Run Locust scenario A against staging Railway"
	@echo "  install-loadtest  pip install locust"

test:
	pytest tests/ -x --tb=short -q

lint:
	ruff check . --config ruff.toml

install-loadtest:
	pip install locust

# Stress test against local backend. Run backend FIRST in another terminal:
#   PYTHONASYNCIODEBUG=1 uvicorn main:app --port 8001 --workers 1
# Then export LOCUST_JWT (use a real staging JWT, never prod) and run `make stress`.
stress:
	@if [ -z "$$LOCUST_JWT" ]; then echo "❌ LOCUST_JWT not set"; exit 2; fi
	rm -f stress_stats.csv stress_stats_history.csv stress-report.html
	LOCUST_HOST=http://localhost:8001 \
	  locust -f loadtests/locustfile_autocomplete_burst.py \
	    --headless --users 22 --spawn-rate 22 --run-time 2m \
	    --csv stress --html stress-report.html
	python loadtests/assert_thresholds.py stress_stats.csv

# Same scenario against Railway staging (web-staging-5f41).
# Use staging JWT only — NEVER hit prod from a stress test.
stress-staging:
	@if [ -z "$$LOCUST_JWT" ]; then echo "❌ LOCUST_JWT not set"; exit 2; fi
	rm -f stress_stats.csv stress_stats_history.csv stress-report.html
	LOCUST_HOST=https://web-staging-5f41.up.railway.app \
	  locust -f loadtests/locustfile_autocomplete_burst.py \
	    --headless --users 22 --spawn-rate 22 --run-time 2m \
	    --csv stress --html stress-report.html
	python loadtests/assert_thresholds.py stress_stats.csv
