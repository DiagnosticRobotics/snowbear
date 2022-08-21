lint:
	flake8 snowbear tests

format:
	black --verbose snowbear tests

isort:
	isort snowbear tests examples

test:
	poetry run pytest --durations=5 tests

build:
	poetry build --format wheel

install:
	poetry install
