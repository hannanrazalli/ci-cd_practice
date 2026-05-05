{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {#- Jika target prod, tambah suffix _prod. Jika tidak, guna nama asal -#}
        {%- if target.name == 'prod' -%}
            {{ custom_schema_name | trim }}_prod
        {%- else -%}
            {{ custom_schema_name | trim }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}