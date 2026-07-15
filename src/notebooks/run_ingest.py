# Databricks notebook source
# Wrapper notebook — calls the Python ingestion script.
# All transformation logic lives in src/ingestion/opensky_stream.py
import sys
catalog = dbutils.widgets.get('catalog')
poll_interval = str(int(float(dbutils.widgets.get('poll_interval'))))
workspace_path = dbutils.widgets.get('workspace_path')
sys.argv = ['opensky_stream.py', '--catalog', catalog, '--poll-interval', poll_interval]
exec(open(f"{workspace_path}/src/ingestion/opensky_stream.py").read())
