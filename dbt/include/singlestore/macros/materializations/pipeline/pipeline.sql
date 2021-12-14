{% materialization pipeline, adapter='singlestore' %}

    -- setup hooks(outside BEGIN transaction)
    {{ run_hooks(pre_hooks, inside_transaction=False) }}

    --  setup hooks(inside BEGIN transaction)
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    -- variables
    {%- set schema = model['schema'] -%}
    {%- set schema = model['schema'] -%}
    {%- set identifier = model['alias'] -%}
    {%- set relation = api.Relation.create(identifier=identifier, schema=schema) -%}

    {%- call statement('main') -%}
        {{ singlestore__create_pipeline_as(relation, sql) }}
    {%- endcall -%}

    {% do log('MATERIALIZATION ' ~ 'PIPELINE ' ~ 'CALLED', True) %}

    -- Return the relations created in this materialization
    {{ return({'relations': [relation]}) }}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    -- `COMMIT` happens here
    {{ adapter.commit() }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

    {{ return({'relations': [target_relation] ,'backup_relation': [backup_relation] }) }}
{% endmaterialization %}