WITH date_range AS (
    SELECT
        MIN(rental_date)::date AS min_date,
        MAX(
            CASE
                WHEN return_date IS NOT NULL THEN return_date::date
                ELSE GREATEST(rental_date::date, CURRENT_DATE) 
            END
        ) AS max_date
    FROM rental
),
all_weeks AS (
    SELECT
        GENERATE_SERIES(
            DATE_TRUNC('week', (SELECT min_date FROM date_range)),
            DATE_TRUNC('week', (SELECT max_date FROM date_range)),
            '1 week'::interval
        )::date AS week_beginning
)
, weekly_returned_counts AS (
    SELECT
        DATE_TRUNC('week', return_date)::date AS week_of_return,
        COUNT(rental_id) AS num_returned_rentals
    FROM rental
    WHERE return_date IS NOT NULL
    GROUP BY 1
)
, weekly_rented_counts AS (
    SELECT
        DATE_TRUNC('week', rental_date)::date AS week_of_rental,
        COUNT(rental_id) AS num_newly_rented
    FROM rental
    GROUP BY 1
)
SELECT
    aw.week_beginning,
    COALESCE(wrc_rented.num_newly_rented, 0) AS newly_rented_during_week,
    COALESCE(wrc_returned.num_returned_rentals, 0) AS returned_rentals_during_week,
    (COALESCE(wrc_rented.num_newly_rented, 0) - COALESCE(wrc_returned.num_returned_rentals, 0)) AS net_change_in_outstanding,
    (
        SELECT COUNT(r_inv.rental_id)
        FROM rental r_inv
        WHERE
            r_inv.rental_date::date <= (aw.week_beginning + '6 days'::interval)::date
            AND (
                r_inv.return_date IS NULL
                OR r_inv.return_date::date > (aw.week_beginning + '6 days'::interval)::date
            )
    ) AS outstanding_rentals_at_week_end
FROM
    all_weeks aw
LEFT JOIN
    weekly_returned_counts wrc_returned ON aw.week_beginning = wrc_returned.week_of_return
LEFT JOIN
    weekly_rented_counts wrc_rented ON aw.week_beginning = wrc_rented.week_of_rental
ORDER BY
    aw.week_beginning;