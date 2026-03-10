-- Likely sellers: high-scoring companies by seller signals
CREATE OR REPLACE VIEW v_likely_sellers AS
SELECT
    c.company_number,
    c.company_name,
    c.company_status,
    c.company_type,
    c.postal_code,
    c.sic_codes,
    c.incorporation_date,
    c.accounts_last_made_up_to,
    cs.signal_score AS seller_score,
    COUNT(DISTINCT a.officer_id) AS active_officer_count,
    MAX(a.appointed_on) AS latest_appointment,
    string_agg(DISTINCT s.signal_type, ', ') AS active_signals
FROM
    companies c
    LEFT JOIN company_signals cs ON c.company_number = cs.company_number AND cs.signal_type = 'seller_score'
    LEFT JOIN appointments a ON c.company_number = a.company_number AND a.is_current = TRUE
    LEFT JOIN company_signals s ON c.company_number = s.company_number AND s.signal_type != 'seller_score'
WHERE
    cs.signal_score >= 70
    AND c.company_status = 'Active'
GROUP BY
    c.company_number,
    c.company_name,
    c.company_status,
    c.company_type,
    c.postal_code,
    c.sic_codes,
    c.incorporation_date,
    c.accounts_last_made_up_to,
    cs.signal_score
ORDER BY
    cs.signal_score DESC,
    c.company_name;

-- Aging founders: companies with directors 55+
CREATE OR REPLACE VIEW v_aging_founders AS
SELECT
    c.company_number,
    c.company_name,
    c.postal_code,
    c.sic_codes,
    o.display_name AS officer_name,
    o.birth_year,
    (EXTRACT(YEAR FROM NOW()) - o.birth_year)::INT AS estimated_age,
    a.role,
    a.appointed_on,
    c.incorporation_date
FROM
    companies c
    JOIN appointments a ON c.company_number = a.company_number
    JOIN officers_resolved o ON a.officer_id = o.officer_id
WHERE
    a.is_current = TRUE
    AND o.birth_year IS NOT NULL
    AND (EXTRACT(YEAR FROM NOW()) - o.birth_year) >= 55
    AND c.company_status = 'Active'
ORDER BY
    estimated_age DESC,
    c.company_name;

-- Succession risk: single director + founder era + no recent appointments
CREATE OR REPLACE VIEW v_succession_risk AS
SELECT
    c.company_number,
    c.company_name,
    c.postal_code,
    c.incorporation_date,
    (EXTRACT(YEAR FROM NOW()) - EXTRACT(YEAR FROM c.incorporation_date))::INT AS years_old,
    o.display_name AS sole_director,
    a.appointed_on,
    c.accounts_last_made_up_to
FROM
    companies c
    JOIN (
        SELECT company_number, officer_id
        FROM appointments
        WHERE is_current = TRUE AND role ILIKE '%director%'
        GROUP BY company_number, officer_id
        HAVING COUNT(*) = 1
    ) single_director ON c.company_number = single_director.company_number
    JOIN appointments a ON c.company_number = a.company_number AND a.is_current = TRUE
    JOIN officers_resolved o ON a.officer_id = o.officer_id
WHERE
    c.company_status = 'Active'
    AND c.incorporation_date <= NOW() - INTERVAL '15 years'
    AND NOT EXISTS (
        SELECT 1
        FROM appointments
        WHERE company_number = c.company_number
            AND appointed_on > NOW() - INTERVAL '5 years'
            AND is_current = TRUE
    )
ORDER BY
    years_old DESC,
    c.company_name;

-- Serial operators: officers linked to 3+ active companies
CREATE OR REPLACE VIEW v_serial_operators AS
SELECT
    o.officer_id,
    o.display_name,
    o.birth_year,
    COUNT(DISTINCT a.company_number) AS active_company_count,
    string_agg(DISTINCT a.company_number, ', ' ORDER BY a.company_number) AS company_numbers,
    string_agg(DISTINCT c.company_name, '; ' ORDER BY c.company_name) AS company_names
FROM
    officers_resolved o
    JOIN appointments a ON o.officer_id = a.officer_id
    JOIN companies c ON a.company_number = c.company_number
WHERE
    a.is_current = TRUE
    AND c.company_status = 'Active'
GROUP BY
    o.officer_id,
    o.display_name,
    o.birth_year
HAVING
    COUNT(DISTINCT a.company_number) >= 3
ORDER BY
    active_company_count DESC,
    o.display_name;

-- Sector map: companies grouped by SIC and postal code prefix
CREATE OR REPLACE VIEW v_sector_map AS
SELECT
    SUBSTRING(c.postal_code, 1, 2) AS postal_region,
    c.sic_codes[1] AS primary_sic_code,
    COUNT(*) AS company_count,
    COUNT(DISTINCT a.officer_id) AS unique_officer_count,
    AVG(
        CASE
            WHEN cs.signal_type = 'seller_score' THEN cs.signal_score::NUMERIC
            ELSE 0
        END
    ) AS avg_seller_score,
    ARRAY_AGG(DISTINCT c.company_status) AS company_statuses
FROM
    companies c
    LEFT JOIN appointments a ON c.company_number = a.company_number AND a.is_current = TRUE
    LEFT JOIN company_signals cs ON c.company_number = cs.company_number
WHERE
    c.postal_code IS NOT NULL
    AND c.sic_codes IS NOT NULL
    AND ARRAY_LENGTH(c.sic_codes, 1) > 0
GROUP BY
    postal_region,
    c.sic_codes[1]
ORDER BY
    postal_region,
    company_count DESC;

-- Family clusters: companies sharing officer surnames
CREATE OR REPLACE VIEW v_family_clusters AS
WITH officer_surnames AS (
    SELECT
        a.company_number,
        o.officer_id,
        SUBSTRING_INDEX(o.display_name, ' ', -1) AS surname,
        o.display_name
    FROM
        appointments a
        JOIN officers_resolved o ON a.officer_id = o.officer_id
    WHERE
        a.is_current = TRUE
),
company_surnames AS (
    SELECT
        company_number,
        surname,
        COUNT(DISTINCT officer_id) AS officer_count,
        string_agg(DISTINCT display_name, ', ') AS officer_names
    FROM
        officer_surnames
    GROUP BY
        company_number,
        surname
    HAVING
        COUNT(DISTINCT officer_id) >= 2
)
SELECT
    cs.surname AS family_name,
    COUNT(DISTINCT cs.company_number) AS company_count,
    string_agg(DISTINCT c.company_name, '; ') AS company_names,
    SUM(cs.officer_count) AS total_family_members,
    AVG(cs.officer_count) AS avg_family_members_per_company
FROM
    company_surnames cs
    JOIN companies c ON cs.company_number = c.company_number
GROUP BY
    cs.surname
HAVING
    COUNT(DISTINCT cs.company_number) >= 2
ORDER BY
    company_count DESC;

-- Job queue status snapshot
CREATE OR REPLACE VIEW v_queue_status AS
SELECT
    status,
    job_type,
    COUNT(*) AS job_count,
    MIN(queued_at) AS oldest_job,
    MAX(finished_at) AS latest_completion,
    AVG(EXTRACT(EPOCH FROM (finished_at - queued_at))) / 60 AS avg_duration_minutes
FROM
    enrichment_jobs
GROUP BY
    status,
    job_type
ORDER BY
    status,
    job_type;

-- Materialized view for likely sellers (refreshable)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_likely_sellers_materialized AS
SELECT
    c.company_number,
    c.company_name,
    c.company_status,
    c.company_type,
    c.postal_code,
    c.sic_codes,
    c.incorporation_date,
    c.accounts_last_made_up_to,
    cs.signal_score AS seller_score,
    COUNT(DISTINCT a.officer_id) AS active_officer_count,
    MAX(a.appointed_on) AS latest_appointment,
    NOW() AS materialized_at
FROM
    companies c
    LEFT JOIN company_signals cs ON c.company_number = cs.company_number AND cs.signal_type = 'seller_score'
    LEFT JOIN appointments a ON c.company_number = a.company_number AND a.is_current = TRUE
WHERE
    cs.signal_score >= 70
    AND c.company_status = 'Active'
GROUP BY
    c.company_number,
    c.company_name,
    c.company_status,
    c.company_type,
    c.postal_code,
    c.sic_codes,
    c.incorporation_date,
    c.accounts_last_made_up_to,
    cs.signal_score;

CREATE INDEX IF NOT EXISTS ix_mv_likely_sellers_seller_score ON mv_likely_sellers_materialized(seller_score DESC);
CREATE INDEX IF NOT EXISTS ix_mv_likely_sellers_postal_code ON mv_likely_sellers_materialized(postal_code);
