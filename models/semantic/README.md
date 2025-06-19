# ZEFIX Semantic Views Architecture

This directory contains focused semantic views for the ZEFIX Swiss company data, designed to enable natural language querying via Snowflake Cortex Analyst.

## 🏗️ **Architecture Overview**

Instead of a single monolithic semantic view, we've created **focused semantic views** that serve specific business purposes:

```
┌─────────────────────────────────────────────────────────┐
│                ZEFIX Data Architecture                  │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  🎯 Focused Semantic Views (models/semantic/)          │
│  ┌─────────────────┬─────────────────┬─────────────────┐ │
│  │ sem_company_    │ sem_publication │ sem_business_   │ │
│  │ overview        │ _activity       │ changes         │ │
│  │                 │                 │                 │ │
│  │ • Legal forms   │ • SHAB records  │ • Mutations     │ │
│  │ • Status        │ • Events        │ • Corporate     │ │
│  │ • Demographics  │ • Temporal      │   events        │ │
│  │                 │   patterns      │                 │ │
│  └─────────────────┴─────────────────┼─────────────────┤ │
│  ┌─────────────────┬─────────────────┘                 │ │
│  │ sem_geographic_ │                                   │ │
│  │ analysis        │                                   │ │
│  │                 │                                   │ │
│  │ • Cantonal      │                                   │ │
│  │   distribution  │                                   │ │
│  │ • Regional      │                                   │ │
│  │   patterns      │                                   │ │
│  └─────────────────┘                                   │ │
│                                                         │
│  🏗️ Silver Layer (Data Processing)                     │
│  • silver_companies                                     │
│  • silver_shab_publications                            │
│  • silver_mutation_types                               │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 📊 **Semantic Views**

### 1. **Company Overview** (`sem_company_overview`)
**Purpose**: Basic company information, legal forms, and operational status

**Best for queries like**:
- "How many active companies are there?"
- "Show me all AG companies"
- "What percentage of companies are active?"
- "How many companies were registered this year?"

**Key dimensions**: Company name, legal form, status, location
**Key metrics**: Company counts, activity rates, legal form distribution

---

### 2. **Publication Activity** (`sem_publication_activity`)  
**Purpose**: SHAB publications, business events, and temporal patterns

**Best for queries like**:
- "How many publications were there this month?"
- "Show formation activity by canton"
- "What's the trend in business dissolutions?"
- "Which types of business events are most common?"

**Key dimensions**: Publication date, activity type, canton, recency
**Key metrics**: Publication volumes, formation/dissolution rates, event types

---

### 3. **Business Changes** (`sem_business_changes`)
**Purpose**: Specific mutations, corporate events, and governance changes

**Best for queries like**:
- "Show me all management changes this year"
- "How many capital increases were there?"
- "Which companies had the most changes?"
- "What types of mutations are most frequent?"

**Key dimensions**: Mutation type, change category, timing
**Key metrics**: Change volumes, mutation frequencies, corporate events

---

### 4. **Geographic Analysis** (`sem_geographic_analysis`)
**Purpose**: Location-based analysis, cantonal distribution, regional patterns

**Best for queries like**:
- "Which canton has the most companies?"
- "Show business activity by region"
- "Compare German vs French speaking regions"
- "What's the concentration of business in major cities?"

**Key dimensions**: Canton, language region, economic zones
**Key metrics**: Regional distribution, concentration ratios, geographic spread

## 🚀 **Usage**

### **Building Semantic Views**
```bash
# Build all semantic views
dbt run --models models/semantic/

# Build specific semantic view
dbt run --models sem_company_overview
dbt run --models sem_publication_activity
dbt run --models sem_business_changes
dbt run --models sem_geographic_analysis
```

### **Natural Language Queries**

Once deployed, you can use Snowflake Cortex Analyst to ask natural language questions:

```sql
-- Example natural language queries:

-- Company Overview
"How many active AG companies are there in Switzerland?"
"What percentage of companies registered this year are still active?"

-- Publication Activity  
"Show me the trend of company formations over the last 5 years"
"Which canton had the most business activity last month?"

-- Business Changes
"How many management changes happened in Zurich this year?"
"What types of corporate events are increasing?"

-- Geographic Analysis
"Which language region has the highest business density?"
"Show me the top 5 cantons by company count"
```

## 🛠️ **Technical Implementation**

### **File Naming Convention**
All semantic view files use the `sem_` prefix:
- `sem_company_overview.sql`
- `sem_publication_activity.sql` 
- `sem_business_changes.sql`
- `sem_geographic_analysis.sql`

### **Custom Materialization**
All semantic views use a custom dbt materialization (`materialization_semantic_view.sql`) that:
- Handles proper Snowflake CREATE SEMANTIC VIEW syntax
- Provides logging and error handling
- Manages view replacement and dependencies

### **Naming Convention**
All semantic expressions follow the Snowflake standard:
```sql
<table_alias>.<semantic_expression_name> AS <sql_expr>
```

### **Clause Ordering**
Following Snowflake requirements:
1. TABLES
2. RELATIONSHIPS  
3. FACTS (before DIMENSIONS)
4. DIMENSIONS
5. METRICS

## 📋 **Creating New Semantic Views**

1. **Copy template**:
   ```bash
   cp models/semantic/sem_company_overview.sql models/semantic/sem_your_new_view.sql
   ```

2. **Update configuration**:
   ```sql
   {{
     config(
       materialized='semantic_view'
     )
   }}
   ```

3. **Define your semantic view**:
   - Choose appropriate tables from Silver layer
   - Define relationships between tables
   - Create meaningful facts, dimensions, and metrics
   - Add comprehensive synonyms for natural language queries

4. **Build and test**:
   ```bash
   dbt run --models sem_your_new_view
   ```

## 🎯 **Best Practices**

### **Synonyms**
- Include multiple ways users might refer to concepts
- Consider both English and domain-specific terms
- Add abbreviations and common variations

### **Comments**
- Provide clear descriptions for all elements
- Explain calculated fields and business logic
- Include examples where helpful

### **Performance**
- Keep semantic views focused on specific use cases
- Avoid overly complex calculated fields
- Consider data volume and query patterns

## 🔧 **Maintenance**

### **Adding New Dimensions/Metrics**
1. Update the appropriate semantic view file
2. Add synonyms for natural language queries
3. Test with sample Cortex Analyst queries
4. Update documentation

### **Schema Changes**
1. Update Silver layer models first
2. Update semantic view references
3. Test all dependent semantic views
4. Deploy in sequence: Silver → Gold

## 📈 **Benefits of This Architecture**

- ✅ **Focused Purpose**: Each view serves specific business needs
- ✅ **Better Performance**: Smaller, targeted queries
- ✅ **Easier Maintenance**: Changes isolated to relevant domains
- ✅ **Clearer Understanding**: Business users can find the right view
- ✅ **Scalable**: Easy to add new views for new use cases
- ✅ **Organized Structure**: Dedicated semantic directory with consistent naming
- ✅ **Reusable**: Custom materialization for consistent implementation 