# What

Monorepo — senior data , software & AI solutions.

- **Root** — Healthcare billing company for arbirtion process
- **Data/** — arbitration related data (calims, provider , docs etc) ingested from various systems, manual clinical entry, and batch database/lab files (no IoT or mobile data sources)

# Domain

- Data analysis using Power BI
- Data reporting on Azure Power BI
- Data engineering using Power BI functions and Azure functions and python
- Secure data using Azure AD and Power BI embedding
- LLM: Claude via LangChain, configurable per chatbot and Microsoft LLM technologies

# Validation
`
- ALWAYS run lint + tests before suggesting a commit

# Critical Rules

- NEVER commit secrets or `.env` files
- NEVER run `sam deploy` or modify CloudFormation without explicit confirmation
- NEVER push to `main` or delete infrastructure without confirmation
- No `// TODO` — implement it or skip it
- Check existing patterns before creating new files

# Workflow Triggers

- "deploy" → `cd yeda-chat/backend && sam build && sam deploy --guided`
- "test all" → run lint + tests for root, yeda-chat/frontend, and yeda-chat/backend
- "dev" → `npm run dev` (root site) or `cd yeda-chat/frontend && npm run dev` (ask which)
- "dev backend" → `cd yeda-chat/backend && python run_local.py`

# Memory

- When I say "remember", add it here
