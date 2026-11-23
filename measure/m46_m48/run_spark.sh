#!/bin/bash
# M-47·M-48 Spark 실험 실행: 4g 상한, 한 번에 하나. new_proj 무거운 컨테이너가 떠 있으면 5분 간격 최대 1시간 기다린다 (같은 PC의 다른 무거운 작업과 겹치지 않게)
# 사용: run_spark.sh <tag> <컨테이너 안 명령>
cd /c/Users/dudfo/Desktop/GH
S=measure/m46_m48; N=logs/m46_m48; mkdir -p $N; TAG=$1; shift  # 스크립트는 S(컨테이너 /n), 출력은 N(git 제외)
for i in $(seq 0 12); do
  busy=$(docker ps --format '{{.Names}}' | grep -E 'duckdb-job|spark-job' || true)
  [ -z "$busy" ] && break
  [ $i -eq 12 ] && { echo "$(date -u +%FT%TZ) $TAG SKIP busy=$busy" | tee -a $N/spark_runs.txt; exit 3; }
  echo "$(date -u +%FT%TZ) $TAG wait busy=$busy" | tee -a $N/spark_runs.txt; sleep 300
done
free=$(df -BG --output=avail /c | tail -1 | tr -dc 0-9)
[ "$free" -lt 100 ] && { echo "$(date -u +%FT%TZ) $TAG SKIP disk=${free}G" | tee -a $N/spark_runs.txt; exit 4; }
s=$(date -u +%FT%TZ); t0=$(date +%s)
MSYS_NO_PATHCONV=1 docker compose -f docker-compose.yml -f $S/ov4g.yml run --rm -v "$(cygpath -w $PWD/$S):/n" bot-job bash -c "
  CG=/sys/fs/cgroup/memory; $*; rc=\$?
  echo \"MEM rc=\$rc max_usage=\$(cat \$CG/memory.max_usage_in_bytes 2>/dev/null) failcnt=\$(cat \$CG/memory.failcnt 2>/dev/null) limit=\$(cat \$CG/memory.limit_in_bytes 2>/dev/null)\"; exit \$rc" > $N/log_$TAG.txt 2>&1
rc=$?
echo "$s $TAG rc=$rc wall=$(( $(date +%s) - t0 ))s free=${free}G $(grep '^MEM' $N/log_$TAG.txt) $(grep '^APP' $N/log_$TAG.txt)" | tee -a $N/spark_runs.txt
exit $rc
