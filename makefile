main:
	pip3 install -r requirement.txt

run:
	gunicorn -c config.py app:app
