# Distributed Systems Engineering: Master Architecture & Learning Plan

This repository is an environment built to master **distributed systems mechanics**: data boundaries, consistency models, network unreliability, asynchronous coordination, and chaos resilience.

---

## 1. System Vision & Core Principles

```
                                  ┌─────────────────────────────┐
                                  │    Client (HTTP Requests)   │
                                  │  (with Idempotency-Key)     │
                                  └──────────────┬──────────────┘
                                                 │ POST /api/v1/orders
                                                 ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│ [ORDER SERVICE] (NestJS + TypeScript) - Saga Orchestrator                              │
│ Port: 3001 | DB: postgres-order (5432)                                                 │
│                                                                                        │
│  ┌─────────────────────────┐          ┌──────────────────────────────────────────┐     │
│  │ Local PostgreSQL        │          │ Outbox Relay Worker (Lease Poller)       │     │
│  │ - orders (State Machine)│─────────▶│ - 2-Phase Status Lease (IN_FLIGHT)       │     │
│  │ - outbox_events         │          │ - Publishes Commands & Events to Kafka   │     │
│  │ - saga_timeouts         │          └────────────────────┬─────────────────────┘     │
│  └─────────────────────────┘                               │                           │
└────────────────────────────────────────────────────────────┼───────────────────────────┘
                                                             │
                                                             ▼
                                      ┌──────────────────────────────────────────────┐
                                      │      Apache Kafka Broker (KRaft Mode)        │
                                      │ Topics: order-events, inventory-events,      │
                                      │         payment-events, *.DLT (Dead Letter)  │
                                      └──────────────┬────────────────────────┬──────┘
                                                     │                        │
                        ┌────────────────────────────┘                        └────────────────────────────┐
                        │ Commands & Events                                                   Commands & Events│
                        ▼                                                                                  ▼
┌──────────────────────────────────────────────────┐               ┌──────────────────────────────────────────────────┐
│ [INVENTORY SERVICE] (Spring Boot 3 / Java 21)    │               │ [PAYMENT / LEDGER SERVICE] (Spring Boot 3/Java21)│
│ Port: 8082 | DB: postgres-inventory (5433)       │               │ Port: 8081 | DB: postgres-payment (5434)         │
│                                                  │               │                                                  │
│ - KafkaListener (order-events)                   │               │ - KafkaListener (payment-commands / order-events)│
│ - Idempotency Guard (processed_events table)     │               │ - Idempotency Guard (processed_events table)     │
│ - Stock Pessimistic Row Lock (FOR UPDATE)        │               │ - Double-Entry Ledger (debits/credits)           │
│ - Ghost-Reservation / Inversion Guard            │               │ - Account Balance Row Lock                       │
│ - Outbox Relay ──▶ Topic: inventory-events       │               │ - Outbox Relay ──▶ Topic: payment-events         │
└───────────────────────┬──────────────────────────┘               └────────────────────────┬─────────────────────────┘
                        │                                                                   │
                        └─────────────────────────────────┬─────────────────────────────────┘
                                                          │ Domain Events
                                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [READ PROJECTION SERVICE] (NestJS / Node.js) - CQRS Read Model                                                      │
│ Port: 3002 | DB: mongodb (27017)                                                                                    │
│                                                                                                                     │
│ - Subscribes to ALL domain events (order-events, inventory-events, payment-events)                                   │
│ - Unordered Event Accumulator (upsert: true) resilient against cross-topic delivery lag                             │
│ - Serves sub-5ms analytics and lookup queries: `GET /api/v1/orders/:id/dashboard`                                  │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### The 4 Golden Rules:
1. **Strict Database-per-Service:** Zero cross-database querying or joins. Services communicate exclusively via Kafka messages or network boundaries.
2. **Asynchronous by Default:** Write operations and state transitions are decoupled via Kafka events. Synchronous HTTP is restricted to immediate client intake and read aggregations.
3. **Fail-Closed & Compensate:** No distributed 2-Phase Commit (2PC). Every distributed workflow is coordinated via an Orchestration-Based Saga with explicit compensating actions.
4. **Resilience to Chaos & Out-of-Order Delivery:** All consumers, pollers, and projection models must handle duplicates, premature compensations, network latency, broker outages, and poison pills gracefully.

---

## 2. Deep Dive: Distributed Patterns & Corner-Case Defenses

### Pattern 1: Transactional Outbox with Non-Blocking Leases
* **Problem Solved (Dual-Write Hazard & DB Connection Pool Starvation):** 
  - Saving state to SQL and publishing to Kafka must be atomic.
  - *Corner Case Hazard:* If an outbox poller holds a database lock (`SELECT FOR UPDATE`) while waiting for a remote Kafka network ACK, broker latency spikes (e.g. 3000ms) will exhaust the database connection pool and crash incoming HTTP traffic.
* **The Solution:**
  1. Business entity and event message are written in the **same local SQL transaction** with `status = 'PENDING'`.
  2. A poller claims a batch using a short lease:
     ```sql
     UPDATE outbox_events
     SET status = 'IN_FLIGHT', lease_expires_at = NOW() + INTERVAL '10 seconds'
     WHERE id IN (
       SELECT id FROM outbox_events
       WHERE (status = 'PENDING' OR (status = 'IN_FLIGHT' AND lease_expires_at < NOW()))
       ORDER BY created_at ASC
       LIMIT 50
       FOR UPDATE SKIP LOCKED
     ) RETURNING *;
     ```
  3. The poller publishes to Kafka asynchronously *outside* of an open database transaction lock.
  4. On Kafka ACK, a lightweight update sets `status = 'PROCESSED', processed_at = NOW()`.
  5. If the worker crashes, the lease expires and another worker safely re-claims the event.

### Pattern 2: Idempotent Consumers & Ingress Deduplication
* **Problem Solved (At-Least-Once Delivery & Double Checkouts):**
  - Kafka guarantees at-least-once delivery; messages can replay on rebalance or network retries.
  - *Corner Case Hazard:* Users double-clicking "Place Order" or network timeouts triggering client retries produce duplicate orders.
* **The Solution:**
  1. **Ingress Layer:** `POST /api/v1/orders` requires an `Idempotency-Key` header. The Order Service rejects or returns cached responses for duplicate client requests.
  2. **Kafka Consumers:** Every event carries a globally unique `eventId` (UUIDv7). Consumers run inside a local SQL transaction that checks and inserts `eventId` into `processed_events`. If already processed, the message is acknowledged and skipped (no-op).

### Pattern 3: Orchestration-Based Saga with Timeout & Inversion Guards
* **Problem Solved (Distributed Consistency & Race Conditions):**
  - Coordinates multi-step transactions across Order, Inventory, and Payment services without distributed locks.
  - **Happy Path:**
    1. Order Service creates order (`ORDER_CREATED`) $\to$ emits `ReserveInventoryCommand`.
    2. Inventory Service locks stock and reserves $\to$ emits `InventoryReservedEvent`.
    3. Order Service consumes `InventoryReservedEvent`, transitions to `PAYMENT_PENDING` $\to$ emits `InitiatePaymentCommand`.
    4. Payment Service debits ledger $\to$ emits `PaymentCapturedEvent`.
    5. Order Service transitions to `ORDER_CONFIRMED` $\to$ emits `OrderConfirmedEvent`.
  - **Compensating Path (Payment Declines):**
    1. Payment Service fails charge $\to$ emits `PaymentFailedEvent`.
    2. Order Service transitions to `COMPENSATING_INVENTORY` $\to$ emits `ReleaseInventoryCommand`.
    3. Inventory Service restores stock $\to$ emits `InventoryReleasedEvent`.
    4. Order Service transitions to `ORDER_CANCELLED`.
  - **Corner Case Hazard 1: Compensation Inversion (Ghost Reservation):**
    - If `ReleaseInventoryCommand` arrives before `ReserveInventoryCommand` due to network delay, Inventory marks the order as `PRE_CANCELLED_TOMBSTONE`. When the late `ReserveInventoryCommand` arrives, it sees the tombstone and immediately aborts reservation without locking stock.
  - **Corner Case Hazard 2: Hanging Sagas (Node Crashes):**
    - If a downstream service crashes and never publishes a reply, the Order Service's **Saga Timeout Poller** detects orders exceeding deadline (`expires_at < NOW()`) and triggers automatic compensation to `ORDER_CANCELLED`.

### Pattern 4: Concurrency Isolation & Oversell Prevention
* **Problem Solved (High-Concurrency Inventory Races):**
  - 100 concurrent requests attempting to buy the last unit of stock.
* **The Solution:**
  - Inventory Service executes stock updates under pessimistic row locking:
    ```sql
    SELECT * FROM products WHERE id = :productId FOR UPDATE;
    ```
  - And enforces database check constraints: `available_stock >= 0`.

### Pattern 5: CQRS Read Model with Unordered Event Accumulation
* **Problem Solved (Cross-Topic Asynchronous Lag & Out-of-Order Delivery):**
  - The Read Projection Service subscribes to 3 distinct Kafka topics (`order-events`, `inventory-events`, `payment-events`).
  - *Corner Case Hazard:* `PaymentCapturedEvent` may arrive in MongoDB *before* `OrderCreatedEvent`.
* **The Solution:**
  - Read projection writes use atomic MongoDB `upsert: true` with `$set` and `$addToSet` for event timelines, allowing any event to create or enrich the document regardless of arrival order.

### Pattern 6: Poison Pills & Dead-Letter Topics (DLT)
* **Problem Solved (Head-of-Line Blocking on Malformed Messages):**
  - A corrupted message or unhandled runtime exception causes infinite consumer retry loops.
* **The Solution:**
  - Spring Kafka and NestJS consumers configure `ErrorHandlingDeserializer` and a maximum retry threshold (3 attempts with exponential backoff).
  - Unrecoverable messages are routed to their respective Dead Letter Topic (e.g. `order-events.DLT`) with exception headers for inspection.

---

## 3. Chaos Engineering Scenarios & Failure Modes

| Chaos Scenario | Method of Injection | Expected Resilience Behavior |
| :--- | :--- | :--- |
| **1. Broker Outage** | `docker stop kafka` while running HTTP load | Orders continue to be accepted; outbox table buffers events with status `PENDING`; zero data loss; poller drains queue once Kafka restarts. |
| **2. Duplicate Message Replay** | Re-publishing identical event payloads | Consumer detects duplicate `eventId` in `processed_events`, skips execution, no double charge or stock leak. |
| **3. Consumer Crash Mid-Batch** | Killing consumer process during processing | Uncommitted offsets reprocess cleanly upon reboot; idempotency guard prevents partial or double execution. |
| **4. Network Latency & Dropped Packets** | Toxiproxy adding 2500ms latency & 30% drop | Outbox lease worker does not block HTTP thread pool; retries back off gracefully. |
| **5. High-Concurrency Stock Race** | 50 concurrent checkouts for 1 item | Pessimistic `FOR UPDATE` serializes reservation; 1 succeeds, 49 fail cleanly with `OUT_OF_STOCK`. |
| **6. Compensation Inversion** | Injecting `ReleaseInventory` before `ReserveInventory` | Inventory tombstone prevents late reservation; zero permanent stock leak. |
| **7. Saga Abandonment / Timeout** | Payment service paused / unrouted | Order Service Saga Timeout poller compensates stuck `PAYMENT_PENDING` orders after 30s. |
| **8. Poison Pill Injection** | Publishing malformed JSON to Kafka | `ErrorHandlingDeserializer` routes message to `.DLT` after 3 retries without blocking partition. |

---

## 4. Technology Stack & Port Allocations

* **NestJS (Node.js/TypeScript):** Order Service (`3001`), Read Projection Service (`3002`)
* **Spring Boot 3 (Java 21):** Payment/Ledger Service (`8081`), Inventory Service (`8082`)
* **Real-Time Visualizer Web App:** Port `3000` (Interactive event flow visualizer, state machine monitor & chaos control)
* **Message Broker:** Apache Kafka (KRaft mode: `localhost:9092`, Kafka UI: `localhost:8080`)
* **Databases:**
  * `postgres-order`: `localhost:5432` (DB: `order_db`)
  * `postgres-inventory`: `localhost:5433` (DB: `inventory_db`)
  * `postgres-payment`: `localhost:5434` (DB: `payment_db`)
  * `mongodb-read`: `localhost:27017` (DB: `read_model_db`)
* **Chaos Proxy:** Toxiproxy (`localhost:8474`)
