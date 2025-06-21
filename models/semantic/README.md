# ZEFIX Semantic Views

Natural language querying for Swiss company data using Snowflake Cortex Analyst.

## 🎯 Purpose

Enable natural language questions like:
- "How many active companies are there?"
- "Show me company formations this year"
- "Which canton has the most AG companies?"

## 📊 Available Views

| View | Purpose | Example Questions |
|------|---------|-------------------|
| `sem_company_overview` | Basic company info | "How many active companies?" |
| `sem_publication_activity` | SHAB publications | "Show formation trends" |
| `sem_business_changes` | Company mutations | "How many management changes?" |
| `sem_geographic_analysis` | Location insights | "Which canton has most companies?" |

## 🚀 Usage

### Build Views
```bash
# Build all semantic views
dbt run --models models/semantic/

# Build specific view
dbt run --models sem_company_overview
```

### Query with Cortex Analyst
Use Snowflake Cortex Analyst to ask natural language questions against these views.

## 🛠️ Technical Details

**Materialization**: Custom `semantic_view` materialization
**Naming**: All files use `sem_` prefix
**Dependencies**: Built on Silver layer models

## Creating New Views

1. Copy existing view as template
2. Update table references and relationships
3. Define facts, dimensions, and metrics
4. Add synonyms for natural language
5. Build and test

## Best Practices

- Use clear, descriptive names
- Include comprehensive synonyms
- Add helpful comments
- Test with sample questions 