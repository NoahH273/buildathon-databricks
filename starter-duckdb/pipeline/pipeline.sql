-- All ordering uses DuckDB's default binary collation; strings stay untrimmed.
CREATE MACRO timestamp_ms(value) AS (
    CASE
        WHEN regexp_full_match(value, '[0-9]+') THEN try_cast(value AS BIGINT)
        WHEN regexp_full_match(value,
            '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(Z|[+-][0-9]{2}:[0-9]{2})')
        AND (right(value, 1) = 'Z' OR
             (try_cast(substr(value, 21, 2) AS INTEGER) <= 23 AND
              try_cast(substr(value, 24, 2) AS INTEGER) <= 59))
        THEN epoch_ms(try_strptime(left(value, 19), '%Y-%m-%dT%H:%M:%S'))
            - CASE WHEN right(value, 1) = 'Z' THEN 0
                   ELSE (try_cast(substr(value, 21, 2) AS BIGINT) * 60
                         + try_cast(substr(value, 24, 2) AS BIGINT)) * 60000
                        * CASE WHEN substr(value, 20, 1) = '+' THEN 1 ELSE -1 END END
    END
);

-- Pick the earliest parsed copy before checking its payload.
CREATE TABLE events AS
WITH deduplicated AS (
    SELECT * FROM raw_events WHERE event_id IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ts) = 1
), parsed AS (
    SELECT e.*, timestamp_ms(ts) AS event_ms, timestamp_ms(ingest_ts) AS ingest_ms,
           CASE WHEN event_type = 'purchase' THEN try_cast(price_cents AS BIGINT)
                ELSE 0 END AS revenue
    FROM deduplicated e
)
SELECT coalesce(p.person_id, e.user_id) AS user_id, event_ms,
       event_type = 'purchase' AS purchase, revenue,
       coalesce(products.category, 'unknown') AS category
FROM parsed e
LEFT JOIN people p ON e.user_id = p.user_id
LEFT JOIN products USING (product_id)
CROSS JOIN config
WHERE e.user_id IS NOT NULL AND product_id IS NOT NULL
  AND event_type IN ('view', 'cart', 'purchase')
  AND event_ms >= window_start AND event_ms < window_end
  AND ingest_ms - event_ms <= 86400000
  AND (event_type <> 'purchase' OR
       (regexp_full_match(price_cents, '[0-9]+') AND revenue BETWEEN 1 AND 10000000));
DROP TABLE raw_events;
-- checkpoint: validate_events

CREATE TABLE clicks AS
WITH deduplicated AS (
    SELECT * FROM raw_clicks WHERE click_id IS NOT NULL
    QUALIFY row_number() OVER (PARTITION BY click_id) = 1
), parsed AS (
    SELECT *, timestamp_ms(ts) AS click_ms FROM deduplicated
)
SELECT click_id, coalesce(p.person_id, c.user_id) AS user_id, campaign_id, click_ms
FROM parsed c
JOIN campaigns USING (campaign_id)
LEFT JOIN people p ON c.user_id = p.user_id
CROSS JOIN config
WHERE c.user_id IS NOT NULL
  AND click_ms >= window_start - 604800000 AND click_ms < window_end;
DROP TABLE raw_clicks;
DROP TABLE people;
-- checkpoint: validate_clicks

-- Only one peer can carry a new gap. RANGE gives every event at the same
-- timestamp the same session number, regardless of the peers' physical order.
CREATE TABLE session_events AS
WITH gaps AS (
    SELECT *, CASE WHEN event_ms - lag(event_ms) OVER (
        PARTITION BY user_id ORDER BY event_ms) <= 1800000 THEN 0 ELSE 1 END AS new_session
    FROM events
)
SELECT *, sum(new_session) OVER (
    PARTITION BY user_id ORDER BY event_ms RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
) AS session_id
FROM gaps;
DROP TABLE events;

CREATE TABLE sessions AS
SELECT user_id, session_id, min(event_ms) AS session_start,
       epoch_ms(min(event_ms))::DATE AS day,
       count(*)::BIGINT AS events, sum(revenue)::BIGINT AS revenue_cents,
       bool_or(purchase) AS purchasing
FROM session_events GROUP BY user_id, session_id;
-- checkpoint: sessionize

CREATE TABLE user_daily AS
WITH category_revenue AS (
    SELECT e.user_id, s.day, e.category, sum(e.revenue) AS revenue
    FROM session_events e JOIN sessions s USING (user_id, session_id)
    WHERE e.purchase GROUP BY e.user_id, s.day, e.category
), top_categories AS (
    SELECT user_id, day, category AS top_category FROM category_revenue
    QUALIFY row_number() OVER (PARTITION BY user_id, day ORDER BY revenue DESC, category) = 1
), daily AS (
    SELECT user_id, day, count(*)::BIGINT AS sessions,
           sum(events)::BIGINT AS events, sum(revenue_cents)::BIGINT AS revenue_cents
    FROM sessions GROUP BY user_id, day
)
SELECT d.*, t.top_category FROM daily d LEFT JOIN top_categories t USING (user_id, day);
DROP TABLE session_events;
-- checkpoint: aggregate_users

-- ASOF avoids expanding every session against every historical click. Resolve
-- click ties first; an expired latest click means all earlier clicks expired.
CREATE TABLE latest_clicks AS
SELECT * FROM clicks
QUALIFY row_number() OVER (PARTITION BY user_id, click_ms ORDER BY click_id) = 1;

CREATE TABLE credited AS
SELECT s.*, CASE WHEN s.session_start - c.click_ms <= 604800000
                 THEN c.campaign_id ELSE 'organic' END AS campaign_id
FROM sessions s ASOF LEFT JOIN latest_clicks c
ON s.user_id = c.user_id AND s.session_start >= c.click_ms;
DROP TABLE latest_clicks;
-- checkpoint: credit_sessions

-- Merge overlapping inclusive seven-day assist intervals before joining to
-- sessions. This prevents repeated clicks (especially whales) expanding rows.
CREATE TABLE assist_intervals AS
WITH distinct_times AS (
    SELECT DISTINCT user_id, campaign_id, click_ms FROM clicks
), gaps AS (
    SELECT *, CASE WHEN click_ms - lag(click_ms) OVER (
        PARTITION BY user_id, campaign_id ORDER BY click_ms) <= 604800000
        THEN 0 ELSE 1 END AS new_interval
    FROM distinct_times
), numbered AS (
    SELECT *, sum(new_interval) OVER (
        PARTITION BY user_id, campaign_id ORDER BY click_ms) AS interval_id
    FROM gaps
)
SELECT user_id, campaign_id, min(click_ms) AS starts, max(click_ms) + 604800000 AS ends
FROM numbered GROUP BY user_id, campaign_id, interval_id;
DROP TABLE clicks;
-- checkpoint: merge_assist_intervals

CREATE TABLE campaign_daily AS
WITH credit_daily AS (
    SELECT campaign_id, day, count(*)::BIGINT AS sessions,
           count(*) FILTER (WHERE purchasing)::BIGINT AS purchasing_sessions,
           sum(revenue_cents)::BIGINT AS revenue_cents
    FROM credited GROUP BY campaign_id, day
), assist_daily AS (
    SELECT a.campaign_id, s.day, count(*)::BIGINT AS assisted_sessions
    FROM sessions s JOIN assist_intervals a
      ON s.user_id = a.user_id AND s.session_start BETWEEN a.starts AND a.ends
    GROUP BY a.campaign_id, s.day
), combined AS (
    SELECT coalesce(c.campaign_id, a.campaign_id) AS campaign_id,
           coalesce(c.day, a.day) AS day,
           coalesce(c.sessions, 0) AS sessions,
           coalesce(c.purchasing_sessions, 0) AS purchasing_sessions,
           coalesce(c.revenue_cents, 0) AS revenue_cents,
           coalesce(a.assisted_sessions, 0) AS assisted_sessions
    FROM credit_daily c FULL OUTER JOIN assist_daily a USING (campaign_id, day)
)
SELECT d.campaign_id,
       CASE WHEN d.campaign_id = 'organic' THEN 'organic' ELSE c.channel END AS channel,
       d.day, d.sessions, d.purchasing_sessions, d.revenue_cents, d.assisted_sessions
FROM combined d LEFT JOIN campaigns c USING (campaign_id);
-- checkpoint: aggregate_campaigns
