# Databricks notebook source
# Wrapper notebook — calls the gold refresh job script.
# All transformation logic lives in src/jobs/gold_refresh.py
import sys
sys.argv = ['gold_refresh.py', '--catalog', dbutils.widgets.get('catalog')]
exec(open(f"{dbutils.widgets.get('workspace_path')}/src/jobs/gold_refresh.py").read())
