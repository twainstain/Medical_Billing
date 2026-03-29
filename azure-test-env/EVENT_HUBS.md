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

A consumer group is an independent reader of the event stream. Each group maintains its own position (offset) in the stream:

```
Event Hub: "claims"
    │
    ├── Consumer Group: $Default ──► Azure Functions (ingestion)
    │   Position: offset 42 (processed up to message 42)
    │
    ├── Consumer Group: analytics ──► ADF / Fabric (real-time analytics)
    │   Position: offset 38 (4 messages behind)
    │
    └── Consumer Group: audit ──► Compliance logger
        Position: offset 42 (caught up)
```

**Our setup:** Basic tier = 1 consumer group ($Default) used by Azure Functions. Production should add groups for analytics and audit.

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

### How Partitions Work

```
Producer sends 3 claims:  CLM-001, CLM-002, CLM-003
                               │
                               ▼
                    ┌── Event Hub: "claims" ──┐
                    │                         │
                    │  Partition 0:            │  Partition 1:
                    │  [CLM-001] [CLM-003]    │  [CLM-002]
                    │                         │
                    └────┬──────────────┬─────┘
                         │              │
                         ▼              ▼
              Function Instance 1    Function Instance 2
              processes CLM-001,     processes CLM-002
              CLM-003 (in order)     (in parallel with P0)
```

**Rules:**
- Messages in the **same partition** are processed **in order** (FIFO)
- Messages in **different partitions** are processed **in parallel**
- **Partition key** determines which partition a message goes to (hash of key % partition count)
- More partitions = more parallelism = higher throughput
- You **cannot reduce** partition count after creation (only increase)

### Partition Keys

Without a key, messages are round-robined across partitions. With a key, same-key messages always go to the same partition:

```python
# Without key — round-robin (default in our system)
batch = producer.create_batch()
batch.add(EventData(edi_content))
producer.send_batch(batch)

# With partition key — same payer's claims go to same partition
batch = producer.create_batch(partition_key="AETNA")
batch.add(EventData(edi_content))
producer.send_batch(batch)
# All AETNA claims go to Partition 0 (hash("AETNA") % 2 = 0)
# All UHC claims go to Partition 1 (hash("UHC") % 2 = 1)
```

**Our system doesn't use partition keys** because ingestion is idempotent — dedup logic in
`shared/dedup.py` means ordering doesn't affect correctness. A claim processed twice produces
the same result. Production may want to add keys by `payer_id` to keep same-payer claims ordered.

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
