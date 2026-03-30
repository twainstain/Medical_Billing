# Report Governance

How to organize, secure, and manage Power BI reports across the billing company.

---

## Workspace Architecture

```
Fabric Workspaces
│
├── medbill-data-platform              Shared data layer (engineering-owned)
│   ├── medbill_lakehouse              Bronze / Silver / Gold Delta tables
│   ├── gold semantic model            Single source of truth for all reports
│   ├── nb_bronze_cdc                  Pipeline notebooks
│   ├── nb_silver_transforms
│   └── nb_gold_aggregations
│
├── medbill-reports-executive          C-suite / leadership reports
│   ├── Executive Dashboard            KPIs, recovery rate, monthly trends
│   ├── Financial Summary              Billed vs paid, underpayment totals
│   └── Monthly Board Report           Month-over-month trends, ROI
│
├── medbill-reports-arbitration        Arbitration team reports
│   ├── Case Pipeline                  Funnel, status, SLA compliance
│   ├── Deadline Compliance            At-risk, missed, met by type
│   ├── Win/Loss Analysis              Outcomes by payer and dispute type
│   └── Arbitration Eligibility        Per-claim drill-through
│
├── medbill-reports-revenue-cycle      Billing / collections reports
│   ├── Claims Aging                   Aging buckets, unpaid amounts
│   ├── Payer Scorecard                Risk tiers, payment rates, denials
│   ├── Denial Management              Denial patterns by payer and code
│   └── CPT Analysis                   Payment ratios vs Medicare benchmarks
│
├── medbill-reports-operations         Operations management reports
│   ├── Analyst Productivity           Cases, win rate, resolution time
│   ├── Provider Performance           Disputes and recovery by provider
│   └── SLA Monitoring                 Real-time deadline tracking
│
└── medbill-reports-sandbox            Development / ad-hoc
    └── (analysts build drafts here before promoting)
```

---

## Key Principles

| Principle | Implementation |
|---|---|
| **One semantic model, many reports** | All reports connect to `gold semantic model` in the data-platform workspace — single source of truth, DAX measures defined once |
| **Separate data from reports** | Data platform workspace is managed by data engineering. Report workspaces are managed by business teams |
| **Audience-based workspaces** | Group by who views, not by topic. This controls access naturally through workspace roles |
| **Promote, don't duplicate** | Analysts build in sandbox workspace, promote to team workspace when approved |
| **No data copies** | All reports use live connection to the shared semantic model. No Import mode, no duplicated datasets |

---

## Shared Semantic Model Pattern

All reports across the company connect to one semantic model:

```
gold semantic model (data-platform workspace)
    │
    │   Live Connection (Direct Lake)
    │
    ├──▶ Executive Dashboard         (executive workspace)
    ├──▶ Financial Summary           (executive workspace)
    ├──▶ Monthly Board Report        (executive workspace)
    ├──▶ Case Pipeline               (arbitration workspace)
    ├──▶ Deadline Compliance         (arbitration workspace)
    ├──▶ Win/Loss Analysis           (arbitration workspace)
    ├──▶ Arbitration Eligibility     (arbitration workspace)
    ├──▶ Claims Aging                (revenue-cycle workspace)
    ├──▶ Payer Scorecard             (revenue-cycle workspace)
    ├──▶ Denial Management           (revenue-cycle workspace)
    ├──▶ CPT Analysis                (revenue-cycle workspace)
    ├──▶ Analyst Productivity        (operations workspace)
    ├──▶ Provider Performance        (operations workspace)
    ├──▶ SLA Monitoring              (operations workspace)
    └──▶ Ad-hoc explorations         (sandbox workspace)
```

Benefits:
- DAX measures defined once, available to all reports
- Gold pipeline updates flow to all reports automatically
- No data duplication, no stale copies
- Consistent metric definitions across teams

### How to Connect a New Report

1. Create new report in any workspace
2. **Get Data** > **Power BI semantic models**
3. Select `gold semantic model` from `medbill-data-platform` workspace
4. Build visuals — all tables and measures are available

---

## Access Control

### Workspace Roles

| Role | Can do |
|---|---|
| **Admin** | Full control: add/remove users, delete workspace |
| **Member** | Create, edit, delete reports. Publish content |
| **Contributor** | Create and edit reports. Cannot delete others' work |
| **Viewer** | View reports only. Cannot edit or download data |

### Role Assignments

| User / Group | data-platform | executive | arbitration | revenue-cycle | operations | sandbox |
|---|---|---|---|---|---|---|
| Data Engineering | Admin | - | - | - | - | Member |
| CEO / CFO | - | Viewer | - | - | - | - |
| VP of Revenue Cycle | - | Viewer | - | Viewer | Viewer | - |
| Arbitration Manager | - | - | Member | - | - | Contributor |
| Arbitration Analysts | - | - | Contributor | - | - | Contributor |
| Revenue Cycle Team | - | - | - | Contributor | - | Contributor |
| Operations Managers | - | - | - | - | Viewer | - |
| All Report Consumers | Viewer* | - | - | - | - | - |

*Viewer on data-platform = can connect to semantic model but cannot see notebooks or pipelines.

### How to Set Up

In Fabric: **Workspace** > **Manage access** > **Add people or groups** > select role.

---

## Row-Level Security (RLS)

For multi-client scenarios (billing company manages multiple provider groups):

### Define RLS Roles in Semantic Model

1. Open `gold semantic model` > **Manage roles**
2. Create roles per provider group:

| Role Name | DAX Filter | Applies To |
|---|---|---|
| Metro General Hospital | `[facility_id] = "FAC001"` | recovery_by_payer, underpayment_detection |
| Riverside Medical Center | `[facility_id] = "FAC002"` | recovery_by_payer, underpayment_detection |
| Heart & Vascular Institute | `[facility_id] = "FAC003"` | recovery_by_payer, underpayment_detection |

### Assign Users to RLS Roles

1. Open semantic model > **Security**
2. Select role > **Add members** > enter email addresses
3. Users see only their provider group's data across all reports

### Test RLS

1. Open semantic model > **Security**
2. Click **Test as role** > select a role
3. Verify the report filters correctly

---

## Naming Convention

```
{team}-{subject}-{type}
```

| Component | Values | Example |
|---|---|---|
| team | `exec`, `arb`, `rev`, `ops` | `arb` |
| subject | descriptive name | `case-pipeline` |
| type | `dashboard`, `report`, `scorecard`, `drillthrough` | `dashboard` |

Examples:
- `exec-financial-summary-dashboard`
- `arb-case-pipeline-dashboard`
- `arb-deadline-compliance-report`
- `rev-claims-aging-report`
- `rev-payer-risk-scorecard`
- `ops-analyst-kpi-scorecard`

---

## Report Catalog

Maintain a catalog so everyone knows what reports exist and who owns them.

### Active Reports

| Report | Workspace | Owner | Audience | Refresh | Description |
|---|---|---|---|---|---|
| Executive Dashboard | executive | Ana Rodriguez | C-suite | 15 min (Direct Lake) | Top-level KPIs: billed, paid, recovery rate, denial rate |
| Financial Summary | executive | Ana Rodriguez | CFO, VP Finance | 15 min | Detailed financial breakdown by payer and period |
| Monthly Board Report | executive | Ana Rodriguez | Board members | Monthly snapshot | Month-over-month trends, arbitration ROI |
| Case Pipeline | arbitration | Mark Thompson | Arb team | 15 min | Case funnel, status distribution, SLA compliance |
| Deadline Compliance | arbitration | Mark Thompson | Arb team | 15 min | NSA deadline tracking: met, missed, at-risk |
| Win/Loss Analysis | arbitration | Sarah Lee | Arb managers | 15 min | Arbitration outcomes by payer and dispute type |
| Arbitration Eligibility | arbitration | Mark Thompson | Arb analysts | 15 min | Per-claim drill-through, QPA comparison |
| Claims Aging | revenue-cycle | Sarah Lee | Billing team | 15 min | Aging buckets with unpaid amounts |
| Payer Scorecard | revenue-cycle | Ana Rodriguez | All teams | 15 min | Payer risk tiers, payment rates, denial rates |
| Denial Management | revenue-cycle | Sarah Lee | Billing team | 15 min | Denial patterns by payer and CARC code |
| CPT Analysis | revenue-cycle | Ana Rodriguez | Coding team | 15 min | Payment ratios vs Medicare/FAIR Health |
| Analyst Productivity | operations | Mark Thompson | Ops managers | 15 min | Cases per analyst, win rate, resolution time |
| Provider Performance | operations | Ana Rodriguez | Provider relations | 15 min | Disputes and recovery by provider/specialty |
| SLA Monitoring | operations | Mark Thompson | Ops managers | 15 min | Real-time deadline and compliance tracking |

### Report Lifecycle

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│  Draft   │────▶│  Review  │────▶│ Promoted │────▶│ Retired  │
│ (sandbox)│     │ (sandbox)│     │  (team)  │     │ (archive)│
└──────────┘     └──────────┘     └──────────┘     └──────────┘
```

1. **Draft**: Analyst builds in sandbox workspace
2. **Review**: Team lead reviews accuracy and design
3. **Promoted**: Published to team workspace, added to catalog
4. **Retired**: Replaced by newer report, moved to archive or deleted

---

## Refresh & Monitoring

### Data Freshness

| Component | Refresh Cycle | Latency |
|---|---|---|
| ADF CDC Pipeline | Every 15 minutes | ~2 min per run |
| Bronze Notebook | Triggered after CDC | ~3 min |
| Silver Notebook | Triggered after Bronze | ~5 min |
| Gold Notebook | Triggered after Silver | ~3 min |
| Direct Lake Reports | Automatic on Delta commit | Instant after Gold completes |
| **End-to-end** | **~15 min cycle** | **~13 min from OLTP change to report** |

### Monitoring

| What to Monitor | Where | Alert Threshold |
|---|---|---|
| Pipeline failures | ADF Monitor | Any failure |
| Notebook errors | Fabric Monitor | Any error in Silver/Gold |
| Report refresh failures | Power BI Service > Monitoring hub | Any failure |
| Data freshness | `_gold_ts` column in Gold tables | > 30 min stale |
| Semantic model size | Fabric capacity metrics | > 1 GB |

### Set Up Alerts

1. **ADF**: Monitor > Alerts > create rule for pipeline failures
2. **Fabric**: Monitor hub > set notification on notebook failures
3. **Power BI**: Workspace > Settings > Notifications > enable refresh failure emails

---

## Governance Checklist

### Initial Setup
- [ ] Create 5 workspaces (data-platform, executive, arbitration, revenue-cycle, operations, sandbox)
- [ ] Deploy `gold semantic model` to data-platform workspace
- [ ] Add DAX measures to semantic model (see `powerbi/REPORT_BUILD_GUIDE.md`)
- [ ] Configure workspace access roles per team
- [ ] Set up RLS roles for multi-provider scenarios

### Per Report
- [ ] Build draft in sandbox workspace
- [ ] Validate metrics against `powerbi/METRICS_GLOSSARY.md`
- [ ] Review with team lead
- [ ] Promote to team workspace
- [ ] Add to report catalog (table above)
- [ ] Assign owner and audience

### Ongoing
- [ ] Review report catalog quarterly — retire unused reports
- [ ] Audit workspace access monthly
- [ ] Monitor pipeline and refresh health daily
- [ ] Update DAX measures when Gold schema changes
- [ ] Train new analysts on naming conventions and promotion process
