{% test hits_row_count(model, column_name) %}
-- count the total rows in source table - newly added rows must equal what is in target table.
-- this logic is testing to ensure we dont have duplicate or missing values using incremental load
WITH 
    target_row_count_and_max_load_tstamp AS
        (
            SELECT 
                COUNT({{ column_name }}) AS target_rows_nbr,
                MAX({{ column_name }}) AS max_loadts
            FROM {{ model }}
        ),

    source_row_counts AS
        (
            SELECT 
                COUNT({{ column_name }}) AS source_row_count,
                target_rows_nbr,
                COUNT(CASE WHEN {{ column_name }} > max_loadts THEN 1 END) AS source_row_count_after_max_loadts
            FROM {{ source('raw_events', 'events') }} e
            INNER JOIN {{ ref('campaign_dim') }}  cd   
                ON LOWER(e.app_id) = LOWER(cd.app_id) AND LOWER(e.page_urlhost) = LOWER(cd.page_urlhost)
            CROSS JOIN target_row_count_and_max_load_tstamp
            WHERE DATE(collector_tstamp, 'US/Arizona') >= '2022-01-01'
                    AND DATE(collector_tstamp, 'US/Arizona') BETWEEN DATE(cd.start_date) AND DATE(cd.end_date)
                AND event = 'unstruct'
                AND event_name = 'affiliate_tracking_2'
            GROUP BY target_rows_nbr
        )
    
    SELECT *
    FROM source_row_counts
    WHERE source_row_count - source_row_count_after_max_loadts != target_rows_nbr


{% endtest %}




