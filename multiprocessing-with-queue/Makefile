.PHONY: test requirements

venv:
	python3 -m venv venv

requirements: venv
	. venv/bin/activate && pip install -r requirements.txt

test: requirements
	. venv/bin/activate && pytest --cov=main.py --cov=src --cov-report=html .
