{% materialization incremental, adapter='databricks' -%}

  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {%- set raw_strategy = config.get('incremental_strategy', default='merge') -%}

  {%- set file_format = dbt_spark_validate_get_file_format(raw_file_format) -%}
  {%- set incremental_strategy = dbt_spark_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}

  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}
  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}

  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {% set tmp_relation = make_temp_relation(this) %}

  {#-- Set Overwrite Mode - does not work for warehouses --#}
  {% if incremental_strategy == 'insert_overwrite' and partition_by %}
    {% call statement() %}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {% endcall %}
  {% endif %}

  {#-- Run pre-hooks --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Incremental run logic --#}
  {% if existing_relation is none %}
    {#-- Relation must be created --#}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
    {#-- Relation must be dropped & recreated, not with delta --#}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {#-- Relation must be merged --#}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do process_schema_changes(on_schema_change, tmp_relation, existing_relation) %}
    {% if partition_by %}
      {%- set get_partitions_query -%}  SELECT DISTINCT {% for partition_key in partition_by %}{{ tmp_relation.include(schema=false) }}.{{partition_key}}{% if not loop.last %}, {% endif %}{% endfor %} FROM {{ tmp_relation.include(schema=false) }} {%- endset -%}
      {%- set partitions = run_query(get_partitions_query).rows -%}
    {%- else -%}
      {%- set partitions = None -%}
    {% endif %}
    {% set build_sql = dbt_databricks_get_incremental_sql(incremental_strategy, tmp_relation, target_relation, unique_key, partition_by, partitions) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {% do persist_docs(target_relation, model) %}

  {% do persist_constraints(target_relation, model) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
