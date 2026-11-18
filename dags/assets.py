"""DAG 사이를 잇는 Asset (implementation_design.md 6절). 이름이 DAG마다 어긋나지 않게 여기서만 정의한다."""
from airflow.sdk import Asset

# 하루치 원천을 다 받았다. extra에 날짜({"day": "YYYY-MM-DD"})를 싣는다. bot_build가 이것을 보고 돈다
RAW_GH = Asset(name="raw.gharchive", uri="file:///work/raw")
