
{% macro mysql__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        select distinct schema_name
        from information_schema.schemata
    {%- endcall %}

    {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro mysql__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {%- endcall -%}
{% endmacro %}

{% macro mysql__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{% macro mysql__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        drop {{ relation.type }} if exists {{ relation }}
    {%- endcall %}
{% endmacro %}

{% macro mysql__truncate_relation(relation) -%}
    {% call statement('truncate_relation') -%}
      truncate table {{ relation }}
    {%- endcall %}
{% endmacro %}

{% macro mysql__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=False) }}
  as
    {{ sql }}
{% endmacro %}

{% macro mysql__current_timestamp() -%}
  current_timestamp()
{%- endmacro %}

{% macro mysql__rename_relation(from_relation, to_relation) -%}
  {# /*
    Singlestore rename fails when the relation already exists.
    Also, it does not support view renames, so a multi-step process is needed:
    1. Drop the existing relation
    2a. If table: rename the new table to existing table name
    2b. If view: create view with existing view name
    */
  #}
  {% call statement('begin_txn') %}
    begin
  {% endcall %}

  {% call statement('drop_relation') %}
    drop {{ to_relation.type }} if exists {{ to_relation }}
  {% endcall %}
  {% if from_relation.type == 'table' %}
    {% call statement('rename_table') %}
      alter table {{ from_relation }} rename to {{ to_relation }}
    {% endcall %}
  {% elif from_relation.type == 'view' %}
    {% call statement('get_view_definition', fetch_result=True) %}
      select view_definition
      from information_schema.views
      where table_schema = '{{ from_relation.schema }}' and table_name = '{{ from_relation.identifier }}'
    {% endcall %}

    {% set view_def = load_result('get_view_definition').data[0][0] %}
    {% call statement('create_view_from_def') %}
      create view {{ to_relation }} as 
      {{ view_def }}
    {% endcall %}

  {% endif %}
  {% call statement('end_txn') %}
    commit
  {% endcall %}

{% endmacro %}

{% macro mysql__check_schema_exists(database, schema) -%}
    {# no-op #}
    {# see MySQLAdapter.check_schema_exists() #}
{% endmacro %}

{% macro mysql__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        show columns from {{ relation.schema }}.{{ relation.identifier }}
    {% endcall %}

    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro mysql__list_relations_without_caching(schema_relation) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      null as "database",
      table_name as name,
      table_schema as "schema",
      case when table_type = 'BASE TABLE' then 'table'
           when table_type = 'VIEW' then 'view'
           else table_type
      end as table_type
    from information_schema.tables
    where table_schema = '{{ schema_relation.schema }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}

{% macro mysql__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}
