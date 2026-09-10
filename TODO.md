# Distributed Systems Project: Step-by-Step Execution Checklist (TODO.md)

This document contains every single task, file, configuration, edge-case mitigation, and verification step required to complete the distributed systems learning environment.

---

## 📋 Phase 1: Infrastructure & Service Scaffolding

### 1.1 Infrastructure Setup (Docker Compose)
- [x] Create `docker-compose.yml` with the following services:
  - [x] `kafka` (Apache Kafka in KRaft mode, port `9092`)
  - [x] `kafka-ui` (Web interface for topics and messages, port `8080`)
  - [x] `postgres-order` (PostgreSQL 16, port `5432`, db: `order_db`)
  - [x] `postgres-inventory` (PostgreSQL 16, port `5433`, db: `inventory_db`)
  - [x] `postgres-payment` (PostgreSQL 16, port `5434`, db: `payment_db`)
  - [x] `mongodb-read` (MongoDB 7, port `27017`, db: `read_model_db`)
  - [x] `toxiproxy` (Shopify Toxiproxy, port `8474`, api: `localhost:8474`)
- [x] Add `.env.example` with standard database credentials and Kafka endpoints.
- [x] Write `scripts/init-kafka-topics.sh` to auto-provision topics and Dead Letter Topics:
  - `order-events` (3 partitions, replication factor 1)
  - `inventory-events` (3 partitions, replication factor 1)
  - `payment-events` (3 partitions, replication factor 1)
  - `order-events.DLT` (1 partition)
  - `inventory-events.DLT` (1 partition)
  - `payment-events.DLT` (1 partition)

### 1.2 Order Service (NestJS)
- [x] Scaffold `services/order-service` using NestJS CLI / TypeScript.
- [x] Configure TypeORM/Prisma with PostgreSQL connection to `localhost:5432`.
- [x] Create initial database migration according to [ARCHITECTURE.md](file:///d:/Distributed_System/ARCHITECTURE.md#L170):
  - `ingress_idempotency_keys` table (key, response_code, response_body, created_at)
  - `orders` table (id, customer_id, idempotency_key UNIQUE, total_amount, currency, status, correlation_id, expires_at, created_at, updated_at)
  - `order_items` table (id, order_id, product_id, quantity, unit_price)
  - `outbox_events` table (id, aggregate_type, aggregate_id, event_type, payload, topic, correlation_id, status, lease_expires_at, retry_count, last_error, created_at, processed_at)
- [x] Implement `POST /api/v1/orders` endpoint with validation pipe and `Idempotency-Key` header check.

### 1.3 Inventory Service (Spring Boot 3 + Java 21)
- [x] Scaffold `services/inventory-service` using Maven with Spring Web, Spring Data JPA, and Spring Kafka.
- [x] Configure Spring Data JPA with PostgreSQL connection to `localhost:5433`.
- [x] Create database migration/schema according to [ARCHITECTURE.md](file:///d:/Distributed_System/ARCHITECTURE.md#L214):
  - `products` table (id, sku UNIQUE, name, total_stock, reserved_stock, available_stock GENERATED, check constraint: non-negative stock)
  - `reservations` table (id, order_id, product_id, quantity, status, created_at)
  - `saga_tombstones` table (order_id PRIMARY KEY, reason, created_at)
  - `processed_events` table (event_id PRIMARY KEY, consumer_name, processed_at)
  - `outbox_events` table (id, aggregate_type, aggregate_id, event_type, payload, topic, correlation_id, status, lease_expires_at, created_at, processed_at)
- [x] Add seed data for 5 sample products.

### 1.4 Payment Service (Spring Boot 3 + Java 21)
- [x] Scaffold `services/payment-service` using Maven/Gradle.
- [x] Configure Spring Data JPA with PostgreSQL connection to `localhost:5434`.
- [x] Create database migration/schema according to [ARCHITECTURE.md](file:///d:/Distributed_System/ARCHITECTURE.md#L258):
  - `accounts` table (id, customer_id UNIQUE, balance, currency, updated_at, check constraint: non-negative balance)
  - `ledger_transactions` table (id, order_id, amount, status, created_at)
  - `ledger_entries` table (id, transaction_id, account_id, entry_type [DEBIT/CREDIT], amount, created_at)
  - `processed_events` table (event_id PRIMARY KEY, consumer_name, processed_at)
  - `outbox_events` table (id, aggregate_type, aggregate_id, event_type, payload, topic, correlation_id, status, lease_expires_at, created_at, processed_at)
- [x] Add seed customer account balances for testing (e.g. `cust_5001` with $500.00, `cust_insufficient` with $5.00).

### 1.5 Read Projection Service (NestJS / Node.js)
- [x] Scaffold `services/read-projection-service`.
- [x] Configure Mongoose / MongoDB connection to `localhost:27017`.
- [x] Define `OrderDashboardSummary` schema supporting partial accumulation.
- [x] Expose `GET /api/v1/orders/:id/dashboard` endpoint and Server-Sent Events (SSE) / WebSocket stream for live UI updates.

### 1.6 Interactive Real-Time Visualizer Web App
- [x] Scaffold `services/visualizer-dashboard` (Port `3000`).
- [x] Build interactive live architecture topology map (animated event paths between Order, Kafka, Inventory, Payment, and MongoDB).
- [x] Build Saga State Machine tracker & order timeline view.
- [x] Build Database & Outbox inspector (view live outbox leases, locks, tombstones, and ledger accounts).
- [x] Build 1-Click Interactive Chaos Control Center (trigger chaos scenarios directly from the UI).

---

## 📋 Phase 2: Non-Blocking Leased Outbox Pattern Implementation

### 2.1 Order Service Outbox Implementation
- [x] Implement `OrderService.createOrder()` using a single database transaction:
  - [x] Write `Order` entity with status `ORDER_CREATED` and deadline `expires_at = NOW() + 60s`.
  - [x] Write `OutboxEvent` with `event_type = 'ReserveInventoryCommand'`, `status = 'PENDING'`, and serialized payload.
- [x] Build Non-Blocking Outbox Poller Worker (`OutboxRelayService`):
  - [x] Run interval poller every 300-500ms.
  - [x] Execute 2-Phase Status Lease in SQL:
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
  - [x] Publish claimed events to Kafka asynchronously outside of open DB row locks.
  - [x] On Kafka `RecordMetadata` acknowledgment, update `status = 'PROCESSED', processed_at = NOW()`.
  - [x] On failure, increment `retry_count`, log `last_error`, and release lease if max retries exceeded.
- [x] Add Outbox Archival/Cleanup job (purge `PROCESSED` events older than 24 hours).

### 2.2 Spring Boot Outbox Implementation (Inventory & Payment)
- [x] Implement JPA repository native lease query with `FOR UPDATE SKIP LOCKED`.
- [x] Implement `@Scheduled` worker with non-blocking async `CompletableFuture` handling on `KafkaTemplate.send()`.
- [x] Mark `PROCESSED` upon Kafka ack callback.

---

## 📋 Phase 3: Idempotent Consumers, Concurrency, & Inversion Guards

### 3.1 Client Ingress Idempotency Middleware (Order Service)
- [x] Create NestJS interceptor / middleware for `POST /api/v1/orders`:
  - [x] Extract `Idempotency-Key` header (reject with 400 Bad Request if missing).
  - [x] Check `ingress_idempotency_keys` table. If cached, return stored response code and body immediately.
  - [x] If new, process order and cache resulting response atomically.

### 3.2 Kafka Consumer Idempotency Guards
- [x] Implement reusable `@Idempotent` pattern and database deduplication:
  - [x] Extract `eventId` from CloudEvents envelope.
  - [x] Execute check and insert into `processed_events` within the local business transaction.
  - [x] If duplicate detected: skip execution and commit Kafka offset (no-op).

### 3.3 High-Concurrency Inventory Isolation & Inversion Guard
- [x] In Inventory Service, implement stock reservation with pessimistic locking:
  ```java
  @Lock(LockModeType.PESSIMISTIC_WRITE)
  @Query("SELECT p FROM Product p WHERE p.id = :id")
  Optional<Product> findByIdWithLock(@Param("id") String id);
  ```
- [x] Implement **Inversion Guard (Ghost Reservation Prevention)**:
  - [x] When `ReleaseInventoryCommand` arrives for an `order_id` that has no active reservation:
    - [x] Insert a record into `saga_tombstones (order_id, reason, created_at)`.
  - [x] When `ReserveInventoryCommand` arrives:
    - [x] Check `saga_tombstones` table first. If a tombstone exists for `order_id`, abort reservation immediately (do not reserve stock) and publish `InventoryReservationFailedEvent (reason: 'PRE_CANCELLED')`.

### 3.4 Double-Entry Ledger Concurrency Guard (Payment Service)
- [x] Lock customer account row (`PESSIMISTIC_WRITE`) during balance check and debit/credit entry writes.
- [x] Verify balance sufficiency; if balance < amount, record failed transaction and publish `PaymentFailedEvent`.

---

## 📋 Phase 4: Orchestration Saga, Timeouts, & Dead-Letter Topics (DLT)

### 4.1 Order Service Saga Orchestrator
- [x] Implement explicit state machine in Order Service handling:
  - [x] `InventoryReservedEvent` $\to$ transition to `PAYMENT_PENDING` + Outbox `InitiatePaymentCommand`.
  - [x] `InventoryReservationFailedEvent` $\to$ transition to `ORDER_CANCELLED`.
  - [x] `PaymentCapturedEvent` $\to$ transition to `ORDER_CONFIRMED` + Outbox `OrderConfirmedEvent`.
  - [x] `PaymentFailedEvent` $\to$ transition to `COMPENSATING_INVENTORY` + Outbox `ReleaseInventoryCommand`.
  - [x] `InventoryReleasedEvent` $\to$ transition to `ORDER_CANCELLED` + Outbox `OrderCancelledEvent`.

### 4.2 Saga Timeout / Dead Man's Switch Poller
- [x] Implement scheduled poller in Order Service (`@Cron('*/10 * * * * *')`):
  - [x] Query orders stuck in `ORDER_CREATED` or `PAYMENT_PENDING` with `expires_at < NOW()`.
  - [x] For timed-out orders: transition to `COMPENSATING_INVENTORY` and publish `ReleaseInventoryCommand`.

### 4.3 Poison Pills & Dead-Letter Topic (DLT) Routing
- [x] Configure Spring Kafka `CommonErrorHandler` with `DeadLetterPublishingRecoverer` and `FixedBackOff(1000L, 3)`:
  - [x] Unrecoverable deserialization or business exceptions forward message to `*.DLT`.
- [x] Configure NestJS Kafka consumer error filter to forward poison pills to `*.DLT`.

---

## 📋 Phase 5: CQRS Read Model Projections & Real-Time Visualization
 
 - [x] In Read Projection Service, subscribe to `order-events`, `inventory-events`, and `payment-events`.
 - [x] Implement unordered atomic upsert handlers (`upsert: true`) in MongoDB using `$set` and `$addToSet`.
 - [x] Build `GET /api/v1/orders/:id/dashboard` endpoint returning denormalized order state + timeline in <5ms.
 - [x] Implement real-time SSE stream (`GET /api/v1/stream/events`) to push live event dispatches to the frontend.
 - [x] Implement Visualizer UI components:
   - [x] Live interactive System Architecture Topology with pulsing event beams.
   - [x] Live Saga Lifecycle tracker showing step-by-step state machine progression and rollback compensations.
   - [x] Outbox & DB Inspector showing outbox leasing (`PENDING` -> `IN_FLIGHT` -> `PROCESSED`) in real time.
   - [x] Interactive Chaos Trigger console to run chaos scripts with live telemetry output.

---

## 📋 Phase 6: Chaos Engineering Lab & Automated Verification

- [x] Create `scripts/chaos/01_broker_outage.sh` and `01_broker_outage.ps1`:
  - Stop Kafka container, submit 50 HTTP orders, verify `PENDING` outbox records, restart Kafka, verify all 50 drain.
- [x] Create `scripts/chaos/02_duplicate_event_replay.sh` and `02_duplicate_event_replay.ps1`:
  - Re-publish identical `PaymentCapturedEvent` 5 times; assert single debit in ledger.
- [x] Create `scripts/chaos/03_compensation_inversion.sh` and `03_compensation_inversion.ps1`:
  - Emit `ReleaseInventoryCommand` before `ReserveInventoryCommand`; verify tombstone prevents ghost reservation.
- [x] Create `scripts/chaos/04_saga_timeout_recovery.sh` and `04_saga_timeout_recovery.ps1`:
  - Create order with payment service disconnected; verify saga timeout worker auto-compensates to `ORDER_CANCELLED`.
- [x] Create `scripts/chaos/05_high_concurrency_oversell.sh` and `05_high_concurrency_oversell.ps1`:
  - Fire 50 concurrent checkouts for a product with 1 available stock; assert exactly 1 success and 49 out-of-stock failures.
- [x] Create `scripts/chaos/06_toxiproxy_latency_resilience.sh` and `06_toxiproxy_latency_resilience.ps1`:
  - Add 3000ms latency on Kafka; verify DB connection pool remains healthy and non-exhausted.
- [x] Create `scripts/chaos/07_poison_pill_dlt.sh` and `07_poison_pill_dlt.ps1`:
  - Send corrupted JSON to `order-events`; verify message moves to `order-events.DLT` without blocking topic.
- [x] Create `scripts/chaos/08_ingress_idempotency.sh` and `08_ingress_idempotency.ps1`:
  - Fire 10 identical HTTP `POST /orders` requests with same `Idempotency-Key`; verify only 1 order created in DB.
- [x] Create `scripts/run-all-chaos-tests.ps1` master suite runner.
