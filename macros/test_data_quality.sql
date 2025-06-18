{% macro test_valid_uid_format(model, column_name) %}
  -- Test that UID follows the correct CHE-###.###.### format
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND NOT REGEXP_LIKE({{ column_name }}, '^CHE-[0-9]{3}\.[0-9]{3}\.[0-9]{3}$')
{% endmacro %}

{% macro test_valid_swiss_zip_code(model, column_name) %}
  -- Test that Swiss ZIP codes are 4 digits
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND NOT REGEXP_LIKE({{ column_name }}, '^[0-9]{4}$')
{% endmacro %}

{% macro test_valid_canton_code(model, column_name) %}
  -- Test that canton codes are valid Swiss canton abbreviations
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NOT NULL
    AND {{ column_name }} NOT IN (
      'AG', 'AI', 'AR', 'BE', 'BL', 'BS', 'FR', 'GE', 'GL', 'GR', 'JU', 'LU',
      'NE', 'NW', 'OW', 'SG', 'SH', 'SO', 'SZ', 'TG', 'TI', 'UR', 'VD', 'VS', 'ZG', 'ZH'
    )
{% endmacro %} 