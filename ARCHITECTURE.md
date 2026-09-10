# Technical Architecture & Event Contracts Specification

This document defines the exact database schemas, Kafka message formats, saga state machine transitions, and resilience mechanics for the distributed systems environment.

---

## 1. Event & Command Contracts (JSON Specifications)

All messages published to Kafka follow a strict CloudEvents / Envelope structure.

### 1.1 Universal Envelope Structure
```json
{
  "eventId": "0191cb61-71b5-7000-8441-6e3e5b306a4b",
  "eventType": "ReserveInventoryCommand",
  "aggregateType": "Order",
  "aggregateId": "ord_1001",
  "timestamp": "2026-09-03T05:24:00.000Z",
  "correlationId": "corr_1001",
  "causationId": "cmd_create_order_001",
  "idempotencyKey": "idem_user_checkout_999",
  "version": 1,
  "payload": {}
}
```

---

### 1.2 Topic: `order-events` (Order Orchestrator & Saga Commands)

#### `OrderCreatedEvent`
```json
{
  "eventId": "uuid-v7",
  "eventType": "OrderCreatedEvent",
  "aggregateType": "Order",
  "aggregateId": "ord_1001",
  "correlationId": "corr_1001",
  "payload": {
    "orderId": "ord_1001",
    "customerId": "cust_5001",
    "totalAmount": 150.00,
    "currency": "USD",
    "items": [
      { "productId": "prod_macbook", "quantity": 1, "unitPrice": 150.00 }
    ]
  }
}
```

#### `ReserveInventoryCommand`
```json
{
  "eventId": "uuid-v7",
  "eventType": "ReserveInventoryCommand",
  "aggregateType": "Order",
  "aggregateId": "ord_1001",
  "correlationId": "corr_1001",
  "payload": {
    "orderId": "ord_1001",
    "items": [
      { "productId": "prod_macbook", "quantity": 1 }
    ]
  }
}
```

#### `InitiatePaymentCommand`
```json
{
  "eventId": "uuid-v7",
  "eventType": "InitiatePaymentCommand",
  "aggregateType": "Order",
  "aggregateId": "ord_1001",
  "correlationId": "corr_1001",
  "payload": {
    "orderId": "ord_1001",
    "customerId": "cust_5001",
    "amount": 150.00,
    "currency": "USD"
  }
}
```

#### `ReleaseInventoryCommand` (Compensating Command)
```json
{
  "eventId": "uuid-v7",
  "eventType": "ReleaseInventoryCommand",
  "aggregateType": "Order",
  "aggregateId": "ord_1001",
  "correlationId": "corr_1001",
  "payload": {
    "orderId": "ord_1001",
    "reason": "PAYMENT_FAILED_INSUFFICIENT_FUNDS"
  }
}
```

#### `OrderConfirmedEvent`
```json
{
  "eventId": "uuid-v7",
  "eventType": "OrderConfirmedEvent",
  "aggregateType": "Order",
  "aggregateId": "ord_1001",
  "correlationId": "corr_1001",
  "payload": {
    "orderId": "ord_1001",
    "confirmedAt": "2026-09-03T05:24:05.000Z"
  }
}
```

#### `OrderCancelledEvent`
```json
{
  "eventId": "uuid-v7",
  "eventType": "OrderCancelledEvent",
  "aggregateType": "Order",
  "aggregateId": "ord_1001",
  "correlationId": "corr_1001",
  "payload": {
    "orderId": "ord_1001",
    "reason": "PAYMENT_FAILED_OR_SAGA_TIMEOUT",
    "cancelledAt": "2026-09-03T05:24:10.000Z"
  }
}
```

---

### 1.3 Topic: `inventory-events`

#### `InventoryReservedEvent`
```json
{
  "eventId": "uuid-v7",
  "eventType": "InventoryReservedEvent",
  "aggregateType": "Inventory",
  "aggregateId": "res_8001",
  "correlationId": "corr_1001",
  "payload": {
    "reservationId": "res_8001",
    "orderId": "ord_1001",
    "warehouseId": "WH-EAST-1",
    "items": [
      { "productId": "prod_macbook", "reservedQuantity": 1 }
    ]
  }
}
```

#### `InventoryReservationFailedEvent`
```json
{
  "eventId": "uuid-v7",
  "eventType": "InventoryReservationFailedEvent",
  "aggregateType": "Inventory",
  "aggregateId": "ord_1001",
  "correlationId": "corr_1001",
  "payload": {
    "orderId": "ord_1001",
    "reason": "OUT_OF_STOCK",
    "productId": "prod_macbook",
    "requestedQuantity": 1,
    "availableQuantity": 0
  }
}
```

#### `InventoryReleasedEvent` (Compensating Result)
```json
{
  "eventId": "uuid-v7",
  "eventType": "InventoryReleasedEvent",
  "aggregateType": "Inventory",
  "aggregateId": "res_8001",
  "correlationId": "corr_1001",
  "payload": {
    "reservationId": "res_8001",
    "orderId": "ord_1001",
    "status": "RELEASED",
    "releasedAt": "2026-09-03T05:24:12.000Z"
  }
}
```

---

### 1.4 Topic: `payment-events`

#### `PaymentCapturedEvent`
```json
{
  "eventId": "uuid-v7",
  "eventType": "PaymentCapturedEvent",
  "aggregateType": "Payment",
  "aggregateId": "tx_3001",
  "correlationId": "corr_1001",
  "payload": {
    "transactionId": "tx_3001",
    "orderId": "ord_1001",
    "customerId": "cust_5001",
    "amount": 150.00,
    "currency": "USD",
    "status": "SUCCESS"
  }
}
```

#### `PaymentFailedEvent`
```json
{
  "eventId": "uuid-v7",
  "eventType": "PaymentFailedEvent",
  "aggregateType": "Payment",
  "aggregateId": "tx_3001",
  "correlationId": "corr_1001",
  "payload": {
    "transactionId": "tx_3001",
    "orderId": "ord_1001",
    "customerId": "cust_5001",
    "amount": 150.00,
    "failureReason": "INSUFFICIENT_FUNDS",
    "status": "FAILED"
  }
}
```

---

### 1.5 Dead Letter Topics (`*.DLT`)
All unrecoverable or malformed payloads are routed to Dead Letter Topics (e.g. `order-events.DLT`, `inventory-events.DLT`, `payment-events.DLT`):
```json
{
  "originalTopic": "order-events",
  "originalEventId": "uuid-v7",
  "failedPayload": "{...}",
  "exceptionClass": "org.springframework.kafka.listener.ListenerExecutionFailedException",
  "exceptionMessage": "Deserialization error or persistent database deadlock",
  "retryCount": 3,
  "failedAt": "2026-09-03T05:25:00.000Z"
}
```

---

## 2. Database DDL Schemas

### 2.1 Order Service DB (`order_db`)
```sql
-- Client Ingress Idempotency Storage
CREATE TABLE ingress_idempotency_keys (
    key VARCHAR(128) PRIMARY KEY,
    response_code INT NOT NULL,
    response_body JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Orders Table
CREATE TABLE orders (
    id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(128) UNIQUE NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    status VARCHAR(32) NOT NULL, 
    -- Statuses: CREATED, INVENTORY_RESERVING, PAYMENT_PENDING, CONFIRMED, COMPENSATING_INVENTORY, CANCELLED
    correlation_id VARCHAR(64) NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL, -- Saga Deadline (e.g. NOW() + 60s)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_orders_saga_timeout ON orders(expires_at) 
WHERE status IN ('CREATED', 'INVENTORY_RESERVING', 'PAYMENT_PENDING', 'COMPENSATING_INVENTORY');

-- Order Items
CREATE TABLE order_items (
    id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id VARCHAR(64) NOT NULL,
    quantity INT NOT NULL,
    unit_price NUMERIC(12, 2) NOT NULL
);

-- Non-Blocking Leased Outbox Events Table
CREATE TABLE outbox_events (
    id VARCHAR(64) PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    topic VARCHAR(64) NOT NULL,
    correlation_id VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING, IN_FLIGHT, PROCESSED, FAILED
    lease_expires_at TIMESTAMP WITH TIME ZONE NULL,
    retry_count INT DEFAULT 0,
    last_error TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE NULL
);

CREATE INDEX idx_outbox_lease ON outbox_events(created_at) 
WHERE status = 'PENDING' OR status = 'IN_FLIGHT';
```

---

### 2.2 Inventory Service DB (`inventory_db`)
```sql
-- Products Stock with Non-Negative Integrity Constraint
CREATE TABLE products (
    id VARCHAR(64) PRIMARY KEY,
    sku VARCHAR(64) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    total_stock INT NOT NULL,
    reserved_stock INT NOT NULL DEFAULT 0,
    available_stock INT GENERATED ALWAYS AS (total_stock - reserved_stock) STORED,
    CONSTRAINT chk_stock_non_negative CHECK (reserved_stock <= total_stock AND reserved_stock >= 0)
);

-- Stock Reservations
CREATE TABLE reservations (
    id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    product_id VARCHAR(64) NOT NULL REFERENCES products(id),
    quantity INT NOT NULL,
    status VARCHAR(32) NOT NULL, -- RESERVED, COMMITTED, RELEASED
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Saga Tombstone Guard (Prevents Ghost Reservations from Inverted Compensations)
CREATE TABLE saga_tombstones (
    order_id VARCHAR(64) PRIMARY KEY,
    reason VARCHAR(128) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Consumer Idempotency Guard
CREATE TABLE processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    consumer_name VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Outbox Events
CREATE TABLE outbox_events (
    id VARCHAR(64) PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    topic VARCHAR(64) NOT NULL,
    correlation_id VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    lease_expires_at TIMESTAMP WITH TIME ZONE NULL,
    retry_count INT DEFAULT 0,
    last_error TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE NULL
);

CREATE INDEX idx_inv_outbox_lease ON outbox_events(created_at) 
WHERE status = 'PENDING' OR status = 'IN_FLIGHT';
```

---

### 2.3 Payment Service DB (`payment_db`)
```sql
-- Accounts (Double-Entry Ledger Account)
CREATE TABLE accounts (
    id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) UNIQUE NOT NULL,
    balance NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT chk_balance_non_negative CHECK (balance >= 0.00)
);

-- Double-Entry Ledger Transactions
CREATE TABLE ledger_transactions (
    id VARCHAR(64) PRIMARY KEY,
    order_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(32) NOT NULL, -- SUCCESS, FAILED
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Ledger Entries (Debit & Credit pairs)
CREATE TABLE ledger_entries (
    id VARCHAR(64) PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL REFERENCES ledger_transactions(id),
    account_id VARCHAR(64) NOT NULL REFERENCES accounts(id),
    entry_type VARCHAR(10) NOT NULL, -- DEBIT or CREDIT
    amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Consumer Idempotency Guard
CREATE TABLE processed_events (
    event_id VARCHAR(64) PRIMARY KEY,
    consumer_name VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Outbox Events
CREATE TABLE outbox_events (
    id VARCHAR(64) PRIMARY KEY,
    aggregate_type VARCHAR(64) NOT NULL,
    aggregate_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    topic VARCHAR(64) NOT NULL,
    correlation_id VARCHAR(64) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    lease_expires_at TIMESTAMP WITH TIME ZONE NULL,
    retry_count INT DEFAULT 0,
    last_error TEXT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed_at TIMESTAMP WITH TIME ZONE NULL
);

CREATE INDEX idx_pay_outbox_lease ON outbox_events(created_at) 
WHERE status = 'PENDING' OR status = 'IN_FLIGHT';
```

---

## 3. Saga State Machine Transition Matrix

| Current State | Event / Trigger | Preconditions & Guard Checks | Action Taken (Outbox / DB) | Next State |
| :--- | :--- | :--- | :--- | :--- |
| `[None]` | `POST /orders` (HTTP) | Unique `Idempotency-Key` validated | Insert Order (`expires_at = NOW()+60s`) + Outbox `ReserveInventoryCommand` | `ORDER_CREATED` |
| `ORDER_CREATED` | `InventoryReservedEvent` | Matches `order_id` & within deadline | Outbox `InitiatePaymentCommand` | `PAYMENT_PENDING` |
| `ORDER_CREATED` | `InventoryReservationFailedEvent`| Stock depleted | Mark cancelled, Outbox `OrderCancelledEvent` | `ORDER_CANCELLED` |
| `ORDER_CREATED` | `SAGA_TIMEOUT` (Cron Poller) | `expires_at < NOW()` | Outbox `ReleaseInventoryCommand` (comp. guard) | `COMPENSATING_INVENTORY` |
| `PAYMENT_PENDING`| `PaymentCapturedEvent` | Payment success | Outbox `OrderConfirmedEvent` | `ORDER_CONFIRMED` |
| `PAYMENT_PENDING`| `PaymentFailedEvent` | Payment declined | Outbox `ReleaseInventoryCommand` | `COMPENSATING_INVENTORY` |
| `PAYMENT_PENDING`| `SAGA_TIMEOUT` (Cron Poller) | `expires_at < NOW()` | Outbox `ReleaseInventoryCommand` | `COMPENSATING_INVENTORY` |
| `COMPENSATING_INVENTORY`| `InventoryReleasedEvent` | Compensation acknowledged | Mark cancelled, Outbox `OrderCancelledEvent` | `ORDER_CANCELLED` |
| `COMPENSATING_INVENTORY`| `SAGA_TIMEOUT` (Retry Poller)| Re-emit release command up to 3x | Outbox `ReleaseInventoryCommand` | `COMPENSATING_INVENTORY` |

---

## 4. CQRS Read Model & MongoDB Accumulation Spec

The Read Model projection service consumes events across all 3 Kafka topics asynchronously. Because events may arrive out of sequence, every event handler performs an atomic **upsert** to accumulate state safely.

### MongoDB Document Schema (`orders_dashboard`)
```json
{
  "_id": "ord_1001",
  "customerId": "cust_5001",
  "status": "CONFIRMED",
  "totalAmount": 150.00,
  "currency": "USD",
  "items": [
    { "productId": "prod_macbook", "quantity": 1, "unitPrice": 150.00 }
  ],
  "inventory": {
    "status": "RESERVED",
    "reservationId": "res_8001",
    "warehouseId": "WH-EAST-1",
    "updatedAt": "2026-09-03T05:24:02.000Z"
  },
  "payment": {
    "status": "SUCCESS",
    "transactionId": "tx_3001",
    "amount": 150.00,
    "updatedAt": "2026-09-03T05:24:04.000Z"
  },
  "timeline": [
    { "event": "OrderCreatedEvent", "timestamp": "2026-09-03T05:24:00.000Z" },
    { "event": "InventoryReservedEvent", "timestamp": "2026-09-03T05:24:02.000Z" },
    { "event": "PaymentCapturedEvent", "timestamp": "2026-09-03T05:24:04.000Z" },
    { "event": "OrderConfirmedEvent", "timestamp": "2026-09-03T05:24:05.000Z" }
  ],
  "lastUpdated": "2026-09-03T05:24:05.000Z"
}
```

### Unordered Projection Logic (Mongoose / MongoDB)
```javascript
// Example for PaymentCapturedEvent arriving potentially before OrderCreatedEvent:
await OrderDashboardModel.updateOne(
  { _id: payload.orderId },
  {
    $set: {
      "payment.status": "SUCCESS",
      "payment.transactionId": payload.transactionId,
      "payment.amount": payload.amount,
      "payment.updatedAt": event.timestamp,
      lastUpdated: new Date()
    },
    $addToSet: {
      timeline: { event: event.eventType, timestamp: event.timestamp }
    }
  },
  { upsert: true }
);
```
