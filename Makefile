.PHONY: install seed app run-case eval demo test clean

install:
	pip install -e ".[dev]"

seed:
	python -m agentic_kpi_analyst.cli seed

app:
	streamlit run src/agentic_kpi_analyst/app/streamlit_app.py

run-case:
	python -m agentic_kpi_analyst.cli run-case --case-id $(CASE_ID)

eval:
	python -m agentic_kpi_analyst.cli eval

demo:
	python -m agentic_kpi_analyst.cli demo

test:
	pytest -v

clean:
	rm -rf data/generated/*.duckdb outputs/reports/* outputs/charts/* outputs/traces/*
