# FlowData

## Create and launch  new venv

`mkdir ~/venv && cd ~/venv && python3 -m venv FlowData && source ~/venv/FlowData/bin/activate`

## Install requirements

`pip install duckdb polars python-dateutil pyarrow`

## Move to working DIR and Clone repo

`mkdir ~/Code && cd ~/Code/ && git clone https://github.com/hgo-digital/FlowData.git`

## Import Supplementary data into DuckDB

`python3 tap_flow_pipeline.py load-hosts --db ./flows.duckdb --hosts ~/Code/FlowData/example/hosts.csv`

`python3 tap_flow_pipeline.py load-dest-map --db ./flows.duckdb --dest-map ~/Code/FlowData/example/dest_app_map.csv`

## Import flows into DuckDB

`python3 tap_flow_pipeline.py import-flows --db ./flows.duckdb --folder ~/Code/FlowData/example/input-data`

## Export enriched flows from DuckDB

`python3 tap_flow_pipeline.py export --db ./flows.duckdb --out ./enriched_flows.parquet`