#!/bin/bash
# Gold 읽기 전용 질의를 4g 컨테이너에서. 사용: q.sh "<sql>" 또는 q.sh 파일.sql (레포 기준 경로)
cd /c/Users/dudfo/Desktop/GH
MSYS_NO_PATHCONV=1 docker compose -f docker-compose.yml -f measure/m46_m48/ov4g.yml run --rm bot-job python -m scripts.query "$1"
