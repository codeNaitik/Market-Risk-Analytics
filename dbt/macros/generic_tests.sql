{# Reusable data tests (no external packages needed, so CI works offline). #}

{% test unique_combination(model, columns) %}
    select {{ columns | join(', ') }}, count(*) as n
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}

{% test positive(model, column_name, allow_zero=false) %}
    select * from {{ model }}
    where {{ column_name }} {{ '<' if allow_zero else '<=' }} 0
{% endtest %}

{% test between(model, column_name, min_value, max_value) %}
    select * from {{ model }}
    where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}

{% test not_in_future(model, column_name) %}
    select * from {{ model }}
    where {{ column_name }} > current_date
{% endtest %}

{% test row_count_at_least(model, min_rows) %}
    select count(*) as n from {{ model }} having count(*) < {{ min_rows }}
{% endtest %}
