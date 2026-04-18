# Project Rules

## General
- Follow the project's existing code style and conventions
- All SQL should be formatted consistently
- Never run destructive queries (DROP, DELETE, TRUNCATE) without explicit confirmation

## Data Engineering
- Use `ref()` instead of hardcoded table names in dbt models
- All staging models must be prefixed with `stg_`
- All intermediate models must be prefixed with `int_`
- All mart models must be prefixed with `fct_` or `dim_`
- Always include a WHERE clause when querying production tables

## Testing
- All new models require at least one `unique` test and one `not_null` test
- Document all model columns with descriptions
- Run `dbt test` before submitting changes

## Code Quality
- Keep SQL queries readable with proper indentation
- Add comments for complex business logic
- Avoid SELECT * in production queries — always specify columns explicitly
