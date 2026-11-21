-- Ingest has already typed candidates, preserving invalid deduplication winners.
-- Both windows use the unique event ID to order peers consistently.
CREATE TABLE session_events AS
WITH gaps AS (
    SELECT *, CASE WHEN event_ms - lag(event_ms) OVER (
        PARTITION BY person_id ORDER BY event_ms, event_id) <= 1800000 THEN 0 ELSE 1 END AS new_session
    FROM events
)
SELECT *, sum(new_session) OVER (
    PARTITION BY person_id ORDER BY event_ms, event_id
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)::BIGINT AS session_no
FROM gaps;
DROP TABLE events;
CREATE TABLE sessions AS
SELECT person_id, session_no, min(event_ms)::BIGINT AS start_ms,
       epoch_ms(min(event_ms))::DATE AS day, count(*)::BIGINT AS events,
       sum(purchase_cents)::BIGINT AS revenue_cents, bool_or(purchase_cents > 0) AS has_purchase
FROM session_events GROUP BY person_id, session_no;
-- checkpoint: duckdb_sessionize

CREATE TABLE user_daily AS
WITH category_revenue AS (
    SELECT e.person_id, s.day, e.category_code, sum(e.purchase_cents) AS revenue
    FROM session_events e JOIN sessions s USING (person_id, session_no)
    WHERE e.purchase_cents > 0 GROUP BY e.person_id, s.day, e.category_code
), top_categories AS (
    SELECT person_id, day, category_code FROM category_revenue
    QUALIFY row_number() OVER (PARTITION BY person_id, day ORDER BY revenue DESC, category_code) = 1
), daily AS (
    SELECT person_id, day, count(*)::BIGINT AS sessions,
           sum(events)::BIGINT AS events, sum(revenue_cents)::BIGINT AS revenue_cents
    FROM sessions GROUP BY person_id, day
)
SELECT d.person_id AS user_id, d.day, d.sessions, d.events, d.revenue_cents, c.category AS top_category
FROM daily d LEFT JOIN top_categories t USING (person_id, day)
LEFT JOIN categories c USING (category_code);
DROP TABLE session_events;
-- checkpoint: duckdb_user_totals

CREATE TABLE people AS
SELECT person_id, (row_number() OVER () - 1)::BIGINT AS person_key,
       min(start_ms) AS first_start, max(start_ms) AS last_start
FROM sessions GROUP BY person_id;
-- checkpoint: duckdb_person_keys
