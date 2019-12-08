all:
	@echo "requirements - generate a new requirements.txt"

requirements:
	pipenv lock --requirements > requirements.txt

