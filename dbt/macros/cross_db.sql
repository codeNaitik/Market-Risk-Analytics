{#
  Cross-database macros. The same models run on DuckDB locally and on Databricks
  (Spark SQL) in the cloud; these macros hide the few syntax differences.
#}

{# Use the custom schema name as-is (gold, staging...) instead of "<target>_gold". #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}{{ target.schema }}{%- else -%}{{ custom_schema_name | trim }}{%- endif -%}
{%- endmacro %}


{# Forward-fill: last non-null value up to the current row. #}
{% macro ffill(column, partition_by, order_by) -%}
    {{ return(adapter.dispatch('ffill')(column, partition_by, order_by)) }}
{%- endmacro %}

{% macro default__ffill(column, partition_by, order_by) -%}
    last_value({{ column }} ignore nulls) over (
        partition by {{ partition_by }} order by {{ order_by }}
        rows between unbounded preceding and current row)
{%- endmacro %}

{% macro databricks__ffill(column, partition_by, order_by) -%}
    last_value({{ column }}, true) over (
        partition by {{ partition_by }} order by {{ order_by }}
        rows between unbounded preceding and current row)
{%- endmacro %}

{% macro spark__ffill(column, partition_by, order_by) -%}
    {{ databricks__ffill(column, partition_by, order_by) }}
{%- endmacro %}


{# One row per calendar day between two date expressions, column name `calendar_date`. #}
{% macro day_spine(start_expr, end_expr) -%}
    {{ return(adapter.dispatch('day_spine')(start_expr, end_expr)) }}
{%- endmacro %}

{% macro default__day_spine(start_expr, end_expr) -%}
    select cast(unnest(generate_series(cast({{ start_expr }} as date), cast({{ end_expr }} as date), interval 1 day)) as date) as calendar_date
{%- endmacro %}

{% macro databricks__day_spine(start_expr, end_expr) -%}
    select explode(sequence(cast({{ start_expr }} as date), cast({{ end_expr }} as date), interval 1 day)) as calendar_date
{%- endmacro %}

{% macro spark__day_spine(start_expr, end_expr) -%}
    {{ databricks__day_spine(start_expr, end_expr) }}
{%- endmacro %}


{# Annualise a daily volatility figure. #}
{% macro annualize_vol(expr) -%}
    ({{ expr }}) * sqrt({{ var('trading_days') }})
{%- endmacro %}


{# ISO day of week: 1 = Monday ... 7 = Sunday #}
{% macro iso_dow(date_expr) -%}
    {{ return(adapter.dispatch('iso_dow')(date_expr)) }}
{%- endmacro %}

{% macro default__iso_dow(date_expr) -%} isodow({{ date_expr }}) {%- endmacro %}
{% macro databricks__iso_dow(date_expr) -%} (weekday({{ date_expr }}) + 1) {%- endmacro %}
{% macro spark__iso_dow(date_expr) -%} (weekday({{ date_expr }}) + 1) {%- endmacro %}
