## Description

<!-- A concise summary of the change and the motivation behind it. Link to any relevant tickets. -->

Closes #<!-- issue number -->

## Type of Change

- [ ] 🐛 Bug fix
- [ ] ✨ New feature / enhancement
- [ ] 🔄 Pipeline / data change
- [ ] 🧪 Test improvement
- [ ] 📄 Documentation update
- [ ] 🏗️ Infrastructure / config change
- [ ] ♻️ Refactor (no functional change)

## Data Impact

<!-- Describe any changes to schemas, table structures, or data outputs. -->

- **Tables changed:** (list any added, modified, or dropped tables)
- **Schema changes:** (describe DDL changes)
- **Backfill required:** Yes / No
- **Downstream impacts:** (Power BI reports, downstream pipelines, etc.)

## Testing

<!-- Describe how you've tested this change. -->

- [ ] Unit tests added / updated (`pytest tests/unit/`)
- [ ] Integration tests run locally (`RUN_INTEGRATION_TESTS=true pytest tests/integration/`)
- [ ] Notebook tested interactively in dev workspace
- [ ] SQL queries validated against dev warehouse
- [ ] Sample data updated if schema changed

## CI Checklist

- [ ] All CI checks pass (lint, unit tests, SQL lint, Bicep validation)
- [ ] No new secrets or credentials introduced
- [ ] `.gitignore` updated if new file types introduced
- [ ] Schema files updated to reflect structural changes

## Reviewer Notes

<!-- Anything you want the reviewer to pay specific attention to. -->
