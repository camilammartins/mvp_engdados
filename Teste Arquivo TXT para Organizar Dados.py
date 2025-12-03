# Databricks notebook source
df_raw = spark.table("pnadc_raw")
display(df_raw.limit(10))