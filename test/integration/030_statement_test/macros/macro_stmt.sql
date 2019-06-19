{% macro load_result_testing(relation) %}
    {%- call statement('test_stmt', fetch_result=True) %}

      select
        count(*) as "num_records"

      from {{ relation }}

    {% endcall -%}
    {% set result = load_result('test_stmt') %}

    {% set res_table = result['table'] %}
    {% set res_matrix = result['data'] %}
    {% for result in res_matrix %}
        {% set matrix_value = res_matrix[loop.index0][0] %}
        {% set table_value = res_table[loop.index0]['num_records'] %}
        select 'matrix' as source, {{ matrix_value }} as value
        union all
        select 'table' as source, {{ table_value }} as value
        {{ "" if loop.last else "union all" }}
    {% endfor %}
{% endmacro %}
