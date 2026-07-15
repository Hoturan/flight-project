# Databricks notebook source
# Wrapper notebook — calls the DQ publish job script.
# All transformation logic lives in src/jobs/dq_publish.py
import sys
sys.argv = ['dq_publish.py', '--catalog', dbutils.widgets.get('catalog')]
exec(open(f"{dbutils.widgets.get('workspace_path')}/src/jobs/dq_publish.py").read())
