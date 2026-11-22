-- ai_agent_ua v1 (범주, 정책 시드에 넣지 않는다): UA가 AI 봇 목록 중 "사람 요청으로 대신 움직이는" 항목에 걸림 (criteria.md 4절)
-- 목적은 robots.json의 function 칸(ref_ua_patterns.purpose). 2026-11-22 판 값 중 AI Assistants, AI Agents, AI Coding Agents,
-- Undocumented AI Agents만 고른다(결정 23, M-25: 72개 이름). 학습용 수집(AI Data Scrapers)과 검색 크롤러(AI Search Crawlers)는 범주에 넣지 않는다
-- 한계: UA만 본다. 서명으로 에이전트를 밝히는 방식(RFC 9421)이 퍼지면 약해진다
WITH e AS (
  SELECT actor_key, client_ua FROM bot.silver.events
  WHERE source = 'auth_lib' AND event_date = DATE(:window_end)
),
d AS (SELECT max(load_date) AS list_date FROM bot.silver.ref_ua_patterns),
p AS (
  SELECT r.pattern, r.name FROM bot.silver.ref_ua_patterns r JOIN d ON r.load_date = d.list_date
  WHERE r.list = 'ai_robots' AND r.purpose IN ('AI Assistants', 'AI Agents', 'AI Coding Agents', 'Undocumented AI Agents')
),
a AS (SELECT actor_key, max(client_ua) AS client_ua FROM e GROUP BY actor_key),
ua AS (
  SELECT u.client_ua, array_sort(collect_set(p.name)) AS names
  FROM (SELECT DISTINCT client_ua FROM a WHERE client_ua IS NOT NULL) u
  JOIN p ON u.client_ua RLIKE p.pattern
  GROUP BY u.client_ua
)
SELECT 'web_client' AS entity_type, a.actor_key AS entity_key,
       DATE(:window_start) AS window_start, DATE(:window_end) AS window_end,
       ua.client_ua IS NOT NULL AS hit,
       CAST(NULL AS DOUBLE) AS score,
       to_json(named_struct('agents', ua.names, 'list_date', d.list_date)) AS metrics
FROM a CROSS JOIN d LEFT JOIN ua ON ua.client_ua = a.client_ua
