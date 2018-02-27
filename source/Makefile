# simple makefile to simplify repetitive build env management tasks under posix
# --no-cache-dir required to solve MD5 mismatch of theano 0.8.2 on Conda.

PYTHON ?= python
PIP ?= pip
PYTEST ?= py.test

install:
	$(PIP) install --no-cache-dir -r requirements.txt

install-develop:
	$(PIP) install --no-cache-dir -e .

install-user:
	$(PIP) install --user --no-cache-dir -r requirements.txt

clean:
	$(PYTHON) setup.py clean --all

test:
	$(PYTEST) tests
