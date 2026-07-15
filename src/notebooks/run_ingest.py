# Databricks notebook source
# Wrapper notebook — calls the Python ingestion script.
# All transformation logic lives in src/ingestion/opensky_stream.py
import sys
sys.argv = ['opensky_stream.py', '--catalog', dbutils.widgets.get('catalog'), '--poll-interval', dbutils.widgets.get('poll_interval')]
exec(open(f"{dbutils.widgets.get('workspace_path')}/src/ingestion/opensky_stream.py").read())
