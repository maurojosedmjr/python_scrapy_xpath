SHELL := /bin/bash
ev=latest

env:
	source venv/bin/activate

black:
	black .

run:
	python scrappy.py