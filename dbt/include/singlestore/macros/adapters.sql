
{% macro singlestore__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        select distinct schema_name
        from information_schema.schemata
    {%- endcall %}

    {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro singlestore__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {%- endcall -%}
{% endmacro %}

{% macro singlestore__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }}
  {% endcall %}
{% endmacro %}

{# --DROP #}
{% macro singlestore__drop_relation(relation) -%}
    {% do log('DROP RELATION', True) %}
    {% do log(relation, True) %}
    {% call statement('drop_relation', auto_begin=False) -%}
        drop {{ relation.type }} if exists {{ relation }}
    {%- endcall %}
{% endmacro %}

{# --TRUNCATE #}
{% macro singlestore__truncate_relation(relation) -%}
    {% call statement('truncate_relation') -%}
      truncate table {{ relation }}
    {%- endcall %}
{% endmacro %}

{# --VIEW #}
{% macro singlestore__create_view_as(relation, sql) -%}
  {% do log('VIEW CREATION', True) %}
  {% do log(relation, True) %}

  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  CREATE VIEW {{ relation.include(database=False) }} as (
    {{ sql }}
  );
{%- endmacro %}

{# --TABLE #}
{% macro singlestore__create_table_as(temporary, relation, sql) %}
  {% do log('TABLE CREATION', True) %}
  {% do log(relation, True) %}

  {%- set is_deleted = config.get('is_deleted', False) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set key_type = config.get('key_type', none) -%}
  {%- set key_columns = config.get('key_columns', none) -%}
  {%- set indexes = config.get('indexes', none) -%}

  {{ sql_header if sql_header is not none }}

  CREATE {% if temporary: -%}temporary{%- endif %}
  TABLE {{ relation.include(database=False) }}

  /* INDEXES */
  {% if indexes is not none %}
    (
    {% for ix in indexes -%}
      {% for ix_name, type_fields in ix.items() %}
        {% for type, fields in type_fields.items() %}
          INDEX {{ ix_name }} ({{ fields }}) USING {{ type }}
        {% endfor %}
      {% endfor %}
      {% if not loop.last %},{% endif %}
    {% endfor %}
    )
  {% endif %}
  
  /* SELECT STATEMENT */
  AS
    {{ sql }};

{% endmacro %}

{# --PIPELINE #}
{% macro singlestore__create_pipeline_as(relation, sql) -%}
  {% do log('MACRO ' ~ 'PIPELINE ' ~ 'CALLED', True) %}
  CREATE OR REPLACE PIPELINE {{ relation.include(database=False) }} AS
    {{ sql }};
{% endmacro -%}

{# --RENAMING #}
{% macro singlestore__rename_relation(from_relation, to_relation) -%}
  {% do log('RENAME MACRO TYPE: (' ~ from_relation.type ~ ')', True) %}
  {% do log('from: ' ~ from_relation, True) %}
  {% do log('to: ' ~ to_relation, True) %} 

  {% call statement('begin_txn') %}
    begin
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

    /* Drop the tmp view (view_name__dbt_tmp) */
    {% call statement('drop_relation') %}
      drop {{ from_relation.type }} if exists {{ from_relation }}
    {% endcall %}

  {% endif %}

  {% call statement('end_txn') %}
    commit
  {% endcall %}

{% endmacro %}

{% macro singlestore__current_timestamp() -%}
  current_timestamp()
{%- endmacro %}

{% macro singlestore__check_schema_exists(database, schema) -%}
    {# no-op #}
    {# see SingleStoreAdapter.check_schema_exists() #}
{% endmacro %}

{% macro singlestore__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        show columns from {{ relation.schema }}.{{ relation.identifier }}
    {% endcall %}

    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro singlestore__list_relations_without_caching(schema_relation) %}
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

{% macro singlestore__generate_database_name(custom_database_name=none, node=none) -%}
  {% do return(None) %}
{%- endmacro %}

{# --Dropping objects #}
{% macro drop_old_relations(schema=target.schema, dryrun=False) %}

{# Get the models that currently exist in dbt #}
{% if execute %}
  {% set current_models=[] %}

  {% for node in graph.nodes.values()
     | selectattr("resource_type", "in", ["model", "seed", "snapshot"])%}
        {% do current_models.append(node.name) %}
    
  {% endfor %}
{% endif %}

{# Run a query to create the drop statements for all relations in BQ that are NOT in the dbt project #}
{% set cleanup_query %}

      WITH MODELS_TO_DROP AS (
          SELECT
            CASE 
              WHEN TABLE_TYPE = 'BASE TABLE' THEN 'TABLE'
              WHEN TABLE_TYPE = 'VIEW' THEN 'VIEW'
            END AS RELATION_TYPE,
            CONCAT('{{ schema }}','.',TABLE_NAME) AS RELATION_NAME
          FROM 
            {{ schema }}.INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = '{{ schema }}'
            AND UPPER(TABLE_NAME) NOT IN
              ({%- for model in current_models -%}
                  '{{ model.upper() }}'
                  {%- if not loop.last -%}
                      ,
                  {% endif %}
              {%- endfor -%})) 
      SELECT 
        'DROP ' || RELATION_TYPE || ' ' || RELATION_NAME || ';' as DROP_COMMANDS
      FROM 
        MODELS_TO_DROP
  {% endset %}

{% set drop_commands = run_query(cleanup_query).columns[0].values() %}

{# Execute each of the drop commands for each relation #}

{% if drop_commands %}
  {% if dryrun | as_bool == False %}
    {% do log('Executing DROP commands...', True) %}
  {% else %}
    {% do log('Printing DROP commands...', True) %}
  {% endif %}
  {% for drop_command in drop_commands %}
    {% do log(drop_command, True) %}
    {% if dryrun | as_bool == False %}
      {% do run_query(drop_command) %}
    {% endif %}
  {% endfor %}
{% else %}
  {% do log('No relations to clean.', True) %}
{% endif %}

{%- endmacro -%}