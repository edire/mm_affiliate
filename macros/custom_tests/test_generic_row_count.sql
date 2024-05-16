{% test generic_row_count(model, column_name, compare_model,time_column,compare_time_column, where_clause) %}



with target_row_cnt as

(

        SELECT 
            COUNT(*) AS actual_row_count
            , MAX({{ compare_time_column }}) AS max_loadts
        FROM {{ model }}

)
    Select * from
        (
            SELECT 
                COUNT(*) AS expected_row_count
                , COUNT(CASE WHEN {{ time_column  }} > max_loadts THEN 1 END) AS source_row_count_after_max_loadts
                , actual_row_count
            FROM {{compare_model}}
            CROSS JOIN target_row_cnt
            WHERE {{where_clause}}
            GROUP BY actual_row_count
        )

    Where expected_row_count - source_row_count_after_max_loadts != actual_row_count


{% endtest %}
