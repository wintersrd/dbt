{% materialization table, default %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = identifier + '__dbt_tmp' -%}
  {%- set backup_identifier = identifier + '__dbt_backup' -%}
  {%- set non_destructive_mode = (flags.NON_DESTRUCTIVE == True) -%}

  {%- set existing_relations = adapter.list_relations(schema=schema) -%}
  {%- set old_relation = adapter.get_relation(relations_list=existing_relations,
                                              schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema, type='table') -%}
  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema, type='table') -%}

  /*
      See ../view/view.sql for more information about this relation.
  */
  {%- set backup_relation = api.Relation.create(identifier=backup_identifier,
                                                schema=schema, type=(old_relation.type or 'table')) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set create_as_temporary = (exists_as_table and non_destructive_mode) -%}


  -- drop the temp relations if they exists for some reason
    {% call statement('drop_existing') %}
        drop table if exists {{ intermediate_relation }};
        drop table if exists {{ backup_relation }};
    {% endcall %}

  -- setup: if the target relation already exists, truncate or drop it (if it's a view)
  {% if non_destructive_mode -%}
    {% if exists_as_table -%}
        {% call statement('truncate existing') %}
            truncate table {{ old_relation }};
        {% endcall %}
    {% elif exists_as_view -%}
        {% call statement('drop view') %}
            drop table {{ old_relation }};
        {% endcall %}
      {%- set old_relation = none -%}
    {%- endif %}
  {%- endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {%- if non_destructive_mode -%}
      {%- if old_relation is not none -%}
        {{ create_table_as(create_as_temporary, intermediate_relation, sql) }}

        {% set dest_columns = adapter.get_columns_in_table(schema, identifier) %}
        {% set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') %}

        insert into {{ target_relation }} ({{ dest_cols_csv }}) (
          select {{ dest_cols_csv }}
          from {{ intermediate_relation.include(schema=(not create_as_temporary)) }}
        );
      {%- else -%}
        {{ create_table_as(create_as_temporary, target_relation, sql) }}
      {%- endif -%}
    {%- else -%}
      {{ create_table_as(create_as_temporary, intermediate_relation, sql) }}
    {%- endif -%}
  {%- endcall %}

  -- cleanup
  {% if non_destructive_mode -%}
    -- noop
  {%- else -%}
    {% if old_relation is not none %}
        {% call statement('rename to backup') %}
            alter table {{ target_relation }} rename to {{ backup_relation }};
        {% endcall %}
    {% endif %}

        {% call statement('rename to new') %}
            alter table {{ intermediate_relation }} rename to {{ target_relation }};
        {% endcall %}
  {%- endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
        {% call statement('commit') %}
            commit;
        {% endcall %}

  -- finally, drop the existing/backup relation after the commit
        {% call statement('drop backup') %}
        drop relation if exists {{ backup_relation }};
        {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
{% endmaterialization %}
