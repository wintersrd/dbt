{% macro snowflake__create_table_as(temporary, relation, sql) -%}
  {% if temporary %}
    use schema {{ adapter.quote_as_configured(schema, 'schema') }};
  {% endif %}

  {%- set transient = config.get('transient', default=true) -%}

  create {% if temporary -%}
    temporary
  {%- elif transient -%}
    transient
  {%- endif %} table {{ relation.include(database=(not temporary), schema=(not temporary)) }}
  as (
    {{ sql }}
  );
{% endmacro %}

{% macro snowflake__create_view_as(relation, sql) -%}
  create or replace view {{ relation }} as (
    {{ sql }}
  );
{% endmacro %}

{% macro snowflake__list_schemas(database) -%}
  {% call statement('list_schemas', fetch_result=True, auto_begin=False) %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.schemata
    where catalog_name='{{ database }}'
  {% endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro snowflake__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}

    show columns in {{ relation }};

    select
        "column_name" as column_name,
        parse_json("data_type"):type::string as data_type,
        parse_json("data_type"):length::int as character_maximum_length,
        parse_json("data_type"):precision::int as numeric_precision,
        parse_json("data_type"):scale::int as numeric_scale

    from table(result_scan(last_query_id(-1)))

  {% endcall %}

  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}

{% endmacro %}


{% macro snowflake__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    {% set database = information_schema.quote_if(information_schema.database, information_schema.should_quote('database')) %}

    -- schema is not quoted properly here
    -- show objects returns MAX 10k records, so we need to filter on schema
    show terse objects in database {{ database }};

    select
        "database_name" as database,
        "name" as name,
        "schema_name" as schema,
        case
            when "kind" = 'TABLE' or "kind" = 'TRANSIENT' then 'table'
            when "kind" = 'VIEW' then 'view'
            when "kind" = 'MATERIALIZED_VIEW' then 'materializedview'
            else 'unknown'
        end as table_type
    from table(result_scan(last_query_id(-1)))
    where "schema_name" ilike '{{ schema }}';

  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro snowflake__check_schema_exists(information_schema, schema) -%}
  {% call statement('check_schema_exists', fetch_result=True) -%}
    {% set database = information_schema.quote_if(information_schema.database, information_schema.should_quote('database')) %}
    show terse schemas in database {{ database }};

    select count(*)
    from table(result_scan(last_query_id(-1)))
    where "schema_name" ilike '{{ schema }}';

  {%- endcall %}
  {{ return(load_result('check_schema_exists').table) }}
{%- endmacro %}

{% macro snowflake__current_timestamp() -%}
  convert_timezone('UTC', current_timestamp())
{%- endmacro %}


{% macro snowflake__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ to_relation }}
  {%- endcall %}
{% endmacro %}
