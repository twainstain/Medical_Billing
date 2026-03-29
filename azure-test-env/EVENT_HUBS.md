# Azure Event Hubs — Deep Dive

> Part of the [Architecture Reference](ARCHITECTURE.md) for the Medical Billing Arbitration platform.

## What It Is

Azure Event Hubs is a **managed event streaming platform** — Microsoft's equivalent of Apache Kafka.
It receives millions of events per second, stores them durably, and lets multiple consumers process
them independently. In our system, it decouples source systems (clearinghouses, payers) from
ingestion functions.

```
Source Systems                     Event Hubs                      Azure Functions
──────────────                     ──────────                      ───────────────
Clearinghouse ──── EDI 837 ──────► claims hub ─────────────────► ingest_claims_function()
Payer Portal  ──── EDI 835 ──────► remittances hub ────────────► ingest_remittances_function()
Doc Intelligence ─ JSON ─────────► documents hub ──────────────► ingest_eob_function()
Workflow Engine ── status ───────► status-changes hub ──────────► (future: notifications)
```

---

## Event Hubs vs Apache Kafka

| Feature | Azure Event Hubs | Apache Kafka |
|---|---|---|
| **Managed** | Fully managed PaaS — no clusters to run | Self-managed (or Confluent Cloud) |
| **Protocol** | AMQP 1.0, HTTPS, **Kafka protocol** (compatible!) | Kafka protocol |
| **Kafka compat** | Yes — Event Hubs exposes a Kafka endpoint. Kafka clients work as-is | Native |
| **Partitions** | 1-32 per hub (Basic), up to 1024 (Dedicated) | Unlimited |
| **Retention** | 1-90 days (Standard), unlimited (Dedicated/Premium) | Configurable |
| **Consumer groups** | Up to 20 (Standard) | Unlimited |
| **Throughput** | 1 MB/s per TU (Throughput Unit) or 1000 events/s | Depends on cluster |
| **Ordering** | Guaranteed within a partition (same as Kafka) | Same |
| **Pricing** | Per Throughput Unit + ingress events | Infrastructure + Confluent license |
| **Schema Registry** | Yes (Avro, built-in) | Yes (Confluent Schema Registry) |
| **Dead-letter** | No built-in (handle in consumer code) | No built-in (same) |
| **Exactly-once** | At-least-once (consumer dedup required) | Exactly-once (with transactions) |

**Key takeaway:** If you know Kafka, you know Event Hubs. You can even use Kafka client libraries
against Event Hubs by pointing to the Kafka-compatible endpoint. The main difference: Event Hubs
is pay-per-use with no infrastructure to manage; Kafka gives you more control but more ops burden.

---

## Our Event Hub Configuration

### 4 Hubs (Topics)

```
Event Hub Namespace: medbill-ehub-*
├── claims          ← EDI 837 from clearinghouses         → ingest_claims_function
├── remittances     ← EDI 835 from clearinghouses/payers  → ingest_remittances_function
├── documents       ← Doc Intelligence extraction results  → ingest_eob_function
└── status-changes  ← Workflow state change events         → (future: notifications)
```

### Settings

| Setting | Our Value | Why |
|---|---|---|
| **Tier** | Basic | Cheapest ($11/mo), sufficient for test volume |
| **Partitions** | 2 per hub | Low volume — 2 gives basic parallelism |
| **Retention** | 1 day | Test env — don't need replay. Production: 7+ days |
| **Consumer groups** | $Default | Single consumer (Azure Functions). Production: add analytics group |
| **Throughput Units** | 1 | 1 TU = 1 MB/s ingress, 2 MB/s egress, 1000 events/s |

### How They're Created

From `scripts/provision.sh:165`:

```bash
# Create namespace (Basic tier)
az eventhubs namespace create \
    --name "$EVENT_HUB_NAMESPACE" \
    --resource-group "$RESOURCE_GROUP" \
    --location "$LOCATION" \
    --sku Basic

# Create 4 hubs with 2 partitions each
for HUB in claims remittances documents status-changes; do
    az eventhubs eventhub create \
        --name "$HUB" \
        --resource-group "$RESOURCE_GROUP" \
        --namespace-name "$EVENT_HUB_NAMESPACE" \
        --partition-count 2
done
```

---

## How Data Gets Into Event Hubs

### Producers (who pushes data in)

| Producer | Method | Hub | Code Reference |
|---|---|---|---|
| Clearinghouse | Event Hub SDK (API push) | `claims` | External system |
| Payer portal | Event Hub SDK or Logic App (SFTP pickup) | `remittances` | External system |
| Doc Intelligence pipeline | Event Hub SDK (after OCR extraction) | `documents` | External system |
| Ingestion functions (downstream events) | `shared/events.py:emit_event()` | Per entity type | `functions/shared/events.py:43` |
| Test simulator | `azure-eventhub` SDK | All hubs | `functions/sample-events/simulate.py:56` |

### Producer Code: `shared/events.py`

After ingestion functions write to Azure SQL, they emit events back to Event Hubs for downstream consumers (workflow engine, CDC, notifications):

```python
# functions/shared/events.py

# Map entity types to Event Hub names
ENTITY_HUB_MAP = {
    "claim": "claims",
    "remittance": "remittances",
    "document": "documents",
    "patient": "status-changes",
    "provider": "status-changes",
    "fee_schedule": "status-changes",
    "case": "status-changes",
    "dispute": "status-changes",
}

def emit_event(event_type, entity_type, entity_id, payload=None):
    """Publish an event to the appropriate Event Hub."""
    hub_name = ENTITY_HUB_MAP.get(entity_type, "status-changes")

    event_body = {
        "event_type": event_type,       # e.g., "claim.insert"
        "entity_type": entity_type,     # e.g., "claim"
        "entity_id": entity_id,         # e.g., "CLM-1001"
        "payload": payload,             # e.g., {"total_billed": 500.00, "payer_id": "AETNA"}
        "emitted_at": "2025-06-15T12:00:00Z",
    }

    producer = EventHubProducerClient.from_connection_string(conn_str, eventhub_name=hub_name)
    batch = producer.create_batch()
    batch.add(EventData(json.dumps(event_body)))
    producer.send_batch(batch)
```

### Producer Code: `simulate.py`

The test simulator pushes sample data to Event Hubs (plays the role of clearinghouse/payer):

```python
# functions/sample-events/simulate.py

def send_to_eventhub(hub_name, data, label):
    """Send data to an Event Hub."""
    conn_str = os.environ["EVENTHUB_CONNECTION_STRING"]
    producer = EventHubProducerClient.from_connection_string(conn_str, eventhub_name=hub_name)
    with producer:
        batch = producer.create_batch()
        batch.add(EventData(data))          # data = raw EDI 837 content as string
        producer.send_batch(batch)

# Send claims
for filename in ["claims_837.edi", "claims_837_replacement.edi"]:
    raw = open(f"sample_data/{filename}").read()
    send_to_eventhub("claims", raw, filename)   # → claims hub → ingest_claims_function
```

---

## How Azure Functions Consume From Event Hubs

### The Trigger Decorator

You register a function to watch a specific Event Hub. Azure calls your function when messages arrive:

```python
# functions/function_app.py

@app.event_hub_message_trigger(
    arg_name="event",                              # the message object
    event_hub_name="claims",                       # which Event Hub to watch
    connection="EVENTHUB_CONNECTION_STRING",        # app setting with connection string
    consumer_group="$Default"                       # consumer group
)
def ingest_claims_function(event: func.EventHubEvent):
    raw_edi = event.get_body().decode("utf-8")     # extract EDI content from event
    from ingest.claims import ingest_claims
    result = ingest_claims(raw_edi)                # parse → validate → dedup → Azure SQL
```

### What Happens Under the Hood

```
1. Producer sends EDI 837 to Event Hub "claims"
         │
2. Azure Functions runtime polls Event Hub (every few seconds)
         │
3. Runtime sees new message, deserializes → EventHubEvent object
         │
4. Runtime calls ingest_claims_function(event)
         │
5. Your code runs:
         │  raw_edi = event.get_body()              ← get EDI content
         │  parser = EDI837Parser(raw_edi)          ← parse segments (parsers/edi_837.py)
         │  claims = parser.parse()                 ← extract claim objects
         │  for claim in claims:
         │      validate_claim(claim)               ← check required fields (validators/validation.py)
         │      resolve_claim_duplicate(claim_id)   ← dedup check (shared/dedup.py)
         │      execute_query("INSERT INTO claims") ← write to Azure SQL (shared/db.py)
         │      emit_event("claim.insert")          ← publish event downstream (shared/events.py)
         │
6. Function returns without error
         │
7. Runtime acknowledges message → removed from Event Hub
         │
   (If function throws → message retried up to 5x → then abandoned)
```

### Consumer Groups

A consumer group is an **independent reader** of the same event stream. Think of it like
**multiple bookmarks in the same book** — each reader is on a different page, but they all
read the same book. Every group sees **all** the messages, but reads at its own pace.

```
Event Hub: "claims"
Messages:  [1] [2] [3] [4] [5] [6] [7] [8] [9] [10]

Consumer Group: $Default (Azure Functions — ingestion)
  ████████████░░░░░░░░  offset=8  (processed 1-8, working on 9)

Consumer Group: analytics (ADF — real-time dashboard)
  ██████░░░░░░░░░░░░░░  offset=6  (2 messages behind — that's OK)

Consumer Group: audit (compliance logger)
  ████████████████████  offset=10 (caught up, read everything)
```

**Key rules:**
- Each group gets **every message** — they don't split work between them
- Each group has its own **checkpoint/offset** — one being slow doesn't block others
- **Without** consumer groups: a second consumer would **compete** with ingestion for messages (both would get only half)
- **With** consumer groups: ingestion and analytics each read **all** messages independently

**Within a single consumer group**, partitions are distributed across function instances
for parallelism (see Partitions section below).

**Our setup:** Basic tier = 1 consumer group ($Default) used by Azure Functions.
Production should upgrade to Standard for 20 groups (analytics, audit, notifications).

**Why this matters for our system:**
- Today: only Azure Functions reads from Event Hub (1 group is enough)
- Future: ADF needs to read the same claims for CDC pipeline (needs a second group)
- Future: Compliance needs to archive all messages (needs a third group)
- Each of these would see every claim, independently, without affecting each other

### Event Hub Event Object

What `event.get_body()` returns depends on what the producer sent:

```python
# For EDI 837 claims:
event.get_body() → b"ISA*00*          *00*          *ZZ*CLEARINGHOUSE..."  # raw EDI bytes

# For downstream events from shared/events.py:
event.get_body() → b'{"event_type":"claim.insert","entity_type":"claim","entity_id":"CLM-1001",...}'

# Properties available on the event object:
event.sequence_number    # 42 (position in partition)
event.offset             # "1024" (byte offset)
event.enqueued_time      # datetime (when Event Hub received it)
event.partition_key       # "AETNA" (if producer set one)
```

---

## Partitions, Ordering, and Parallelism

### What Is a Partition?

A partition is an **ordered sequence of messages** within an Event Hub. Think of it like
**lanes on a highway** — more lanes = more cars can travel in parallel, but within each
lane cars stay in order.

```
Event Hub: "claims" (2 partitions)

                    ┌────────── Partition 0 ──────────┐
                    │ [msg1] [msg3] [msg5] [msg7] ... │  ← FIFO within lane
                    └─────────────────────────────────┘
                    ┌────────── Partition 1 ──────────┐
                    │ [msg2] [msg4] [msg6] [msg8] ... │  ← FIFO within lane
                    └─────────────────────────────────┘
                                                         ← parallel across lanes
```

**Why partitions matter:**
- **1 partition** = 1 consumer can process at a time = serial (slow)
- **2 partitions** = 2 consumers can process in parallel = 2x throughput
- **8 partitions** = 8 consumers in parallel = 8x throughput
- You **cannot reduce** partitions after creation (only increase)

### How Messages Get Assigned to Partitions

```
Producer sends claim                    Which partition?
─────────────────────                   ────────────────

No partition key (round-robin):         msg1→P0, msg2→P1, msg3→P0, msg4→P1...
  batch = producer.create_batch()       (spread evenly, no ordering guarantee
  batch.add(EventData(edi_content))      across messages)

With partition key:                     hash("AETNA") % 2 = 0 → always P0
  batch = producer.create_batch(        hash("UHC") % 2 = 1   → always P1
      partition_key="AETNA")            (same key = same partition = ordered)
  batch.add(EventData(edi_content))
```

### How Our Data Is Partitioned Today

**We use round-robin (no partition key)** — messages are distributed evenly:

```
Clearinghouse sends 6 claims:

  CLM-001 (Anthem)  ──► Partition 0
  CLM-002 (Aetna)   ──► Partition 1
  CLM-003 (UHC)     ──► Partition 0
  CLM-004 (Anthem)  ──► Partition 1    ← Anthem claims in DIFFERENT partitions
  CLM-005 (Aetna)   ──► Partition 0       (no ordering between Anthem claims)
  CLM-006 (UHC)     ──► Partition 1

Azure Functions:
  Instance 1 reads P0: CLM-001, CLM-003, CLM-005 (in order within P0)
  Instance 2 reads P1: CLM-002, CLM-004, CLM-006 (in order within P1)
  Both run in PARALLEL
```

**Why this works for us:** Our ingestion is **idempotent** — the dedup logic in
`shared/dedup.py` means a claim processed twice produces the same result. We don't
need same-payer claims to arrive in order because:
- `resolve_claim_duplicate()` checks frequency_code (1=insert, 7=update, 8=void)
- `check_remittance_duplicate()` checks (claim_id, trace_number, payer_id)
- If the same claim arrives twice, the second one is skipped

### How We SHOULD Partition in Production

For production with higher volume, partition by `payer_id` to:
1. Keep same-payer claims in order (avoids edge cases in dedup)
2. Isolate slow payers from fast ones (one payer backup doesn't block others)
3. Enable per-payer monitoring and throttling

```
With partition_key = payer_id:

  CLM-001 (Anthem)  ──► Partition 0    ← All Anthem claims together, in order
  CLM-004 (Anthem)  ──► Partition 0
  CLM-002 (Aetna)   ──► Partition 1    ← All Aetna claims together, in order
  CLM-005 (Aetna)   ──► Partition 1
  CLM-003 (UHC)     ──► Partition 0    ← UHC lands wherever hash("UHC") % N sends it
  CLM-006 (UHC)     ──► Partition 0

Code change needed in shared/events.py:
  batch = producer.create_batch(partition_key=str(payer_id))  # add this
```

With 8 payers and 8 partitions, each payer would get roughly its own lane.

### Partition Count: How to Choose

| Volume | Recommended Partitions | Why |
|---|---|---|
| Test (10 claims/day) | **2** (our current) | Minimal — just proves parallelism works |
| Low (100 claims/day) | **4** | 2 partitions per consumer instance |
| Medium (1,000/day) | **8** | 1 partition per payer |
| High (10,000+/day) | **16-32** | Max parallelism for concurrent clearinghouse feeds |

**Our setup:** 2 partitions per hub. Created in `scripts/provision.sh:186`:
```bash
az eventhubs eventhub create --name "$HUB" --partition-count 2
```

### Partitions vs Consumer Groups (Common Confusion)

These are orthogonal concepts:

```
                        Partitions = parallelism WITHIN a group
                        Consumer groups = independent readers of ALL data

Event Hub: "claims" (4 partitions)
│
├── Consumer Group: $Default (Azure Functions)
│   ├── Instance A reads Partition 0, 1     ← partitions split across instances
│   └── Instance B reads Partition 2, 3        for parallel processing
│
└── Consumer Group: analytics (ADF)
    └── Single reader reads ALL partitions  ← different group, same data
```

- **Partitions** control how much **parallelism** a single consumer group gets
- **Consumer groups** control how many **independent readers** see the same data
- More partitions = faster processing within one group
- More consumer groups = more use cases reading the same stream

---

## Tier Comparison & Limitations

| Feature | Basic ($11/mo) | Standard ($33/mo) | Premium ($$$) | Dedicated ($$$$) |
|---|---|---|---|---|
| **Max message size** | 256 KB | 1 MB | 1 MB | 1 MB |
| **Partitions per hub** | 32 | 32 | 100 | 1024 |
| **Consumer groups** | 1 ($Default) | 20 | 100 | 100+ |
| **Retention** | 1 day | 7 days | 90 days | 90 days |
| **Throughput** | 1 TU (shared) | Up to 40 TU | Auto-scale | Dedicated CU |
| **Capture to ADLS** | No | Yes | Yes | Yes |
| **Kafka endpoint** | No | Yes | Yes | Yes |
| **Schema Registry** | No | Yes | Yes | Yes |
| **VNet / Private Link** | No | No | Yes | Yes |
| **Geo-disaster recovery** | No | Yes | Yes | Yes |

### Key Limitations for Our System

| Limitation | Impact | Mitigation |
|---|---|---|
| **256 KB message limit** | Large EDI 837 files (100+ claims) may exceed limit | Split files into claim-level batches before sending |
| **1 consumer group** | Only Azure Functions can read; no analytics tap | Upgrade to Standard ($33/mo) for 20 groups |
| **No Kafka endpoint** | Can't use Kafka client libs from clearinghouses | Use `azure-eventhub` SDK; or upgrade to Standard |
| **No Capture** | Can't auto-archive to ADLS for replay/audit | Upgrade to Standard and enable Capture |
| **1 day retention** | If Function App is down >24h, messages are lost | Upgrade to Standard (7 days) or Premium (90 days) |
| **No dead-letter built-in** | Failed messages need manual handling | Our DLQ table (`dead_letter_queue`) handles this at app level |

### Message Size: What Fits in 256 KB?

| Content | Typical Size | Fits in Basic? |
|---|---|---|
| Single EDI 837 claim (5 lines) | 1-3 KB | Yes |
| EDI 837 batch (10 claims) | 10-30 KB | Yes |
| EDI 837 batch (100 claims) | 100-300 KB | Maybe (borderline) |
| EDI 837 batch (500 claims) | 500 KB+ | No — split needed |
| FHIR Patient JSON | 2-5 KB | Yes |
| Doc Intelligence extraction | 5-20 KB | Yes |
| EOB PDF (raw binary) | 500 KB - 5 MB | No — use Blob Storage instead |

**Our approach:** EDI files go through Event Hub (small). PDF/images go through Blob Storage trigger (any size).

---

## Event Flow: Complete Lifecycle

```
EXTERNAL SYSTEM                 EVENT HUB                    AZURE FUNCTION                  AZURE SQL
───────────────                 ─────────                    ──────────────                  ─────────

Clearinghouse                   claims hub                   ingest_claims_function          claims table
sends EDI 837  ──── push ─────► [partition 0] ── trigger ──► parse EDI 837                  claim_lines table
                                [partition 1]                 validate claim
                                                              check dedup                     remittances table
                                                              INSERT INTO claims ──────────► (rows created)
                                                              │
                                                              ▼
                                status-changes hub           emit_event("claim.insert")
                                [partition 0] ◄── push ───── (downstream notification)
                                                              │
                                                              ▼
                                                              return (success)
                                                              │
                                                              ▼
                                                             message acknowledged
                                                             (removed from hub)
```

### Error Handling

```
If ingest_claims_function() throws an exception:
    │
    ├── Retry 1: Azure retries immediately
    ├── Retry 2: Azure retries after backoff
    ├── Retry 3-5: more retries with increasing delay
    │
    └── After max retries: message is ABANDONED
        (lost unless you implement checkpoint recovery)

Our mitigation:
    - Validation errors → caught before throw → sent to dead_letter_queue table
    - Only unexpected errors (DB connection, etc.) cause retries
    - Dead-letter queue: shared/dlq.py → analysts triage pending failures
```

---

## Monitoring & Debugging

### View Event Hub Metrics

```bash
# Check namespace health
az eventhubs namespace show --name medbill-ehub-* --resource-group rg-medbill-test

# List hubs and partition count
az eventhubs eventhub list --namespace-name medbill-ehub-* --resource-group rg-medbill-test -o table

# Check incoming message count (Azure Portal → Event Hub → Metrics → Incoming Messages)
```

### Stream Function Logs (see messages being processed)

```bash
func azure functionapp logstream medbill-func-8df6df9c
# Output:
# [2025-03-29] Claims function triggered, body length: 1842
# [2025-03-29] Claims ingestion complete: {"inserted": 4, "updated": 0, "errors": 0}
```

### Test With Simulator

```bash
cd functions/sample-events
pip install azure-eventhub azure-storage-blob python-dotenv

# Send all sample data
python simulate.py --all

# Or individual pipelines
python simulate.py --claims         # EDI 837 → Event Hub "claims"
python simulate.py --remittances    # EDI 835 → Event Hub "remittances"
python simulate.py --patients       # FHIR JSON → HTTP POST (not Event Hub)
python simulate.py --documents      # PDF → Blob Storage (not Event Hub)
python simulate.py --fee-schedules  # CSV → Blob Storage (not Event Hub)
```

---

## Production Recommendations

| Change | Why | Cost Impact |
|---|---|---|
| Upgrade to **Standard** tier | Kafka endpoint, 7-day retention, Capture, 20 consumer groups | +$22/mo |
| Increase to **4-8 partitions** | Handle concurrent clearinghouse feeds | Free (just config) |
| Enable **Capture** to ADLS | Auto-archive all messages for replay and HIPAA audit trail | ~$2/mo storage |
| Add **partition key** by `payer_id` | Keep same-payer claims ordered (helps dedup performance) | Free (code change) |
| Add **analytics consumer group** | Let ADF/Fabric read events without affecting ingestion | Free (just config) |
| Set **retention to 7 days** | Allows replay if ingestion function had a bug over the weekend | Included in Standard |
| Add **Azure Monitor alerts** | Alert on message backlog, ingestion failures, throttling | ~$1/mo |

---

## How Event Hub Compares to Other Azure Messaging

| Service | Pattern | Ordering | Retention | Best For |
|---|---|---|---|---|
| **Event Hubs** | Pub/sub streaming | Per partition | Days-months | High-throughput event ingestion (our use case) |
| **Service Bus** | Queue / topic | FIFO | 14 days | Transactional messaging, request-reply, guaranteed delivery |
| **Event Grid** | Reactive push | None | 24h retry | Resource events (blob created, deployment done) |
| **Storage Queue** | Simple queue | FIFO | 7 days | Cheap background job queue |

**Why we chose Event Hub:**
- Native Azure Functions trigger integration (zero-config consumer)
- Kafka-compatible (clearinghouses may already have Kafka clients)
- Handles high burst throughput from batch EDI file processing
- Cheapest option for event streaming at our volume

---

## Code Reference Summary

| File | Purpose | Key Functions |
|---|---|---|
| `scripts/provision.sh:165` | Creates Event Hub namespace + 4 hubs | `az eventhubs namespace create`, `az eventhubs eventhub create` |
| `functions/function_app.py` | Registers Event Hub triggers | `@app.event_hub_message_trigger(event_hub_name="claims")` |
| `functions/shared/events.py` | Publishes events downstream | `emit_event(event_type, entity_type, entity_id, payload)` |
| `functions/sample-events/simulate.py` | Test producer | `send_to_eventhub(hub_name, data, label)` |
| `functions/ingest/claims.py` | Claims consumer logic | `ingest_claims(raw_edi)` — called by trigger |
| `functions/ingest/remittances.py` | Remittances consumer logic | `ingest_era(raw_edi)`, `ingest_eob(eob_json)` |
| `functions/shared/dedup.py` | Idempotency (makes ordering irrelevant) | `resolve_claim_duplicate()`, `check_remittance_duplicate()` |
| `functions/shared/dlq.py` | App-level dead-letter queue | `send_to_dlq(source, entity_type, error_category, ...)` |
