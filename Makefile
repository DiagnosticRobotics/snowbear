lint:
	flake8 snowbear

format:
	black --verbose snowbear

isort:
	isort snowbear tests examples

test:
	poetry run pytest --durations=5 tests

build:
	poetry build --format wheel

install:
	poetry install
