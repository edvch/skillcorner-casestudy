# SkillCorner Case Study

Small, reproducible pipeline that ingests football data from SkillCorner’s API, stores and processes the data to create analysis-ready tables. These tables should be used in a small streamlit app to visualize the data.

- Code that runs the pipeline locally 
- Streamlit app deployed on the native streamlit server and has a secure login (username password)


## Project structure

- [casestudy/](casestudy) — core package and data

  - [casestudy/pipeline/ingestion] - Fetch and load data
      - [casestudy/pipeline/aggregation/] - Processing scripts to normalize and clean tables
  - [casestudy/pipeline/aggregation/] - Aggregation script data calling SkillCorner's DynamicEventsAggretor
  - Data directories: [casestudy/data](casestudy/data) (raw / processed / metadata / analytics)

- [interface/main.py](interface/main.py) — project interface entry
- [streamlit/streamlit_app.py](streamlit/streamlit_app.py) — Streamlit demo app
- [casestudy/notebooks/casestudy.ipynb](casestudy/notebooks/casestudy.ipynb) — exploratory notebooks
- [requierements.txt](requierements.txt) — Python dependencies
- [Makefile](Makefile)

## Setup

1. Create a virtual environment and install dependencies:
```sh
python -m venv .venv
source .venv/bin/activate
pip install -r [requierements.txt]

## File overview

- casestudy/pipeline/ingestion/
    - fetch_data.py - Calling SkillCorner APIs to collect matches infos and Dynamic Events base on competion id and store the files locally
    - load_data.py - Load the data in order to process and aggregate them

- casestudy/pipeline/processing
    - processing.py - Normalise and clean data before aggregating them

- casestudy/pipeline/aggregation
    - aggregation.py - Create SkillCorner DynamicEventsAggretor instance to aggregate collected data