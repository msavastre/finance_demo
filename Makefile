PYTHON ?= python3

.PHONY: install bootstrap run

install:
	$(PYTHON) -m pip install -r requirements.txt

bootstrap:
	$(PYTHON) scripts/bootstrap_bq.py

run:
	streamlit run ui/streamlit_app.py

