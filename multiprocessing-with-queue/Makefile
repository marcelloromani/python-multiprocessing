.PHONY: test lint requirements help
.DEFAULT_GOAL=help
.NOTPARALLEL:

venv:
	python3 -m venv venv

requirements: venv
	. venv/bin/activate && pip install -r requirements.txt

lint: requirements
	. venv/bin/activate && pylint `git ls-files '*.py'`

test: requirements
	. venv/bin/activate && pytest --cov=main.py --cov=src --cov-report=html .

help:
	@echo "test             Run tests"
	@echo "requirements     Install requirements"
	@echo "venv             Initialise virtualenv"
