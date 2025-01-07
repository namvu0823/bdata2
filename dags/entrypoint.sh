#!/bin/bash
set -e

# Khởi tạo cơ sở dữ liệu Airflow
airflow db upgrade

# Tạo người dùng admin
airflow users create \
  --username admin \
  --password admin \
  --firstname Airflow \
  --lastname Admin \
  --role Admin \
  --email admin@example.com

# Khởi động webserver
exec airflow webserver