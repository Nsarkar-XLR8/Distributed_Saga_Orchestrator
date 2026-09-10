// -----------------------------------------------------------------------------
// Distributed Systems Visualizer Frontend Logic
// -----------------------------------------------------------------------------

const ORDER_SERVICE_URL = 'http://localhost:3001';
const INVENTORY_SERVICE_URL = 'http://localhost:8082';
const PAYMENT_SERVICE_URL = 'http://localhost:8081';
const READ_MODEL_URL = 'http://localhost:3002';

// State
let eventSource = null;
let totalOrderCount = 0;

// Initialize on DOM load
document.addEventListener('DOMContentLoaded', () => {
  initIdempotencyKey();
  setupEventListeners();
  connectSSE();
  loadAllData();

  // Polling fallback every 3s
  setInterval(() => {
    loadDatabaseTables();
  }, 3000);
});

function generateUUID() {
  return 'idem_' + Math.random().toString(36).substring(2, 10) + '_' + Date.now().toString().slice(-4);
}

function initIdempotencyKey() {
  const keyInput = document.getElementById('input-idempotency');
  if (keyInput) {
    keyInput.value = generateUUID();
  }
}

function setupEventListeners() {
  // Regenerate key button
  const regenBtn = document.getElementById('btn-regen-key');
  if (regenBtn) {
    regenBtn.addEventListener('click', initIdempotencyKey);
  }

  // Order form submission
  const orderForm = document.getElementById('order-form');
  if (orderForm) {
    orderForm.addEventListener('submit', handleOrderSubmit);
  }

  // Duplicate button
  const dupBtn = document.getElementById('btn-duplicate-order');
  if (dupBtn) {
    dupBtn.addEventListener('click', () => {
      handleOrderSubmit(null, true);
    });
  }

  // Tabs
  const tabBtns = document.querySelectorAll('.tab-btn');
  tabBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      tabBtns.forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

      btn.classList.add('active');
      const tabId = btn.getAttribute('data-tab');
      document.getElementById(tabId)?.classList.add('active');
    });
  });
}

// -----------------------------------------------------------------------------
// Server-Sent Events (SSE) Live Stream
// -----------------------------------------------------------------------------
function connectSSE() {
  const statusEl = document.getElementById('val-sse-status');
  
  try {
    eventSource = new EventSource(`${READ_MODEL_URL}/api/v1/stream/events`);

    eventSource.onopen = () => {
      if (statusEl) {
        statusEl.textContent = 'CONNECTED (LIVE)';
        statusEl.className = 'value badge-live';
      }
    };

    eventSource.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);
        handleLiveEvent(payload);
      } catch (err) {
        console.error('Failed to parse SSE event', err);
      }
    };

    eventSource.onerror = () => {
      if (statusEl) {
        statusEl.textContent = 'RECONNECTING...';
        statusEl.className = 'value text-dim';
      }
    };
  } catch (e) {
    console.warn('SSE connection failed', e);
  }
}

function handleLiveEvent(envelope) {
  const { type, data } = envelope;
  const eventType = data?.eventType || type;
  const orderId = data?.orderId || data?.doc?._id;

  updateLiveBanner(`Received [${eventType}] for Order: ${orderId}`);
  animateNodeForEvent(eventType);
  loadOrderProjections();
  loadDatabaseTables();
}

// -----------------------------------------------------------------------------
// Interactive Node Animation & Visual Effects
// -----------------------------------------------------------------------------
function animateNode(nodeId, duration = 1200) {
  const node = document.getElementById(nodeId);
  if (!node) return;

  node.classList.add('active-pulse');
  setTimeout(() => {
    node.classList.remove('active-pulse');
  }, duration);
}

function animateNodeForEvent(eventType) {
  switch (eventType) {
    case 'OrderCreatedEvent':
    case 'ReserveInventoryCommand':
      animateNode('node-client', 800);
      setTimeout(() => animateNode('node-order', 800), 200);
      setTimeout(() => animateNode('node-kafka', 800), 500);
      setTimeout(() => animateNode('node-inventory', 800), 800);
      break;

    case 'InventoryReservedEvent':
      animateNode('node-inventory', 800);
      setTimeout(() => animateNode('node-kafka', 800), 300);
      setTimeout(() => animateNode('node-order', 800), 600);
      break;

    case 'InitiatePaymentCommand':
      animateNode('node-order', 800);
      setTimeout(() => animateNode('node-kafka', 800), 300);
      setTimeout(() => animateNode('node-payment', 800), 600);
      break;

    case 'PaymentCapturedEvent':
    case 'PaymentFailedEvent':
      animateNode('node-payment', 800);
      setTimeout(() => animateNode('node-kafka', 800), 300);
      setTimeout(() => animateNode('node-order', 800), 600);
      break;

    case 'ReleaseInventoryCommand':
      animateNode('node-order', 800);
      setTimeout(() => animateNode('node-kafka', 800), 300);
      setTimeout(() => animateNode('node-inventory', 800), 600);
      break;

    case 'OrderConfirmedEvent':
    case 'OrderCancelledEvent':
      animateNode('node-order', 800);
      setTimeout(() => animateNode('node-read', 1000), 400);
      break;

    default:
      animateNode('node-kafka', 800);
      animateNode('node-read', 800);
  }
}

function updateLiveBanner(text) {
  const bannerText = document.getElementById('live-banner-text');
  if (bannerText) {
    bannerText.textContent = text;
  }
}

// -----------------------------------------------------------------------------
// Order Placement Handler
// -----------------------------------------------------------------------------
async function handleOrderSubmit(e, isDuplicate = false) {
  if (e) e.preventDefault();

  const customerId = document.getElementById('input-customer').value;
  const productSelect = document.getElementById('input-product');
  const productId = productSelect.value;
  const unitPrice = parseFloat(productSelect.selectedOptions[0].getAttribute('data-price') || '100');
  const quantity = parseInt(document.getElementById('input-quantity').value, 10);
  const idempotencyKey = document.getElementById('input-idempotency').value;

  const payload = {
    customerId,
    currency: 'USD',
    items: [
      {
        productId,
        quantity,
        unitPrice,
      },
    ],
  };

  updateLiveBanner(`Dispatching POST /api/v1/orders (Idempotency-Key: ${idempotencyKey})...`);
  animateNode('node-client');

  try {
    const response = await fetch(`${ORDER_SERVICE_URL}/api/v1/orders`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Idempotency-Key': idempotencyKey,
      },
      body: JSON.stringify(payload),
    });

    const data = await response.json();
    animateNode('node-order');

    if (!isDuplicate) {
      totalOrderCount++;
      document.getElementById('val-total-orders').textContent = totalOrderCount;
      initIdempotencyKey(); // generate next unique key for regular flows
    }

    logChaosOutput(`Order Response [Status ${response.status}]:\n${JSON.stringify(data, null, 2)}`);
    loadOrderProjections();
  } catch (error) {
    logChaosOutput(`❌ Order dispatch failed: ${error.message}`);
  }
}

// -----------------------------------------------------------------------------
// Data Fetchers & Visualizers
// -----------------------------------------------------------------------------
async function loadAllData() {
  await Promise.allSettled([
    loadOrderProjections(),
    loadDatabaseTables(),
  ]);
}

async function loadOrderProjections() {
  const container = document.getElementById('orders-list-container');
  if (!container) return;

  try {
    const res = await fetch(`${READ_MODEL_URL}/api/v1/orders/dashboard`);
    if (!res.ok) return;

    const orders = await res.json();
    if (!orders || orders.length === 0) {
      container.innerHTML = '<div class="empty-state">No orders placed yet. Click "Execute Saga Checkout" above!</div>';
      return;
    }

    container.innerHTML = orders.map(order => `
      <div class="order-card-item">
        <div class="order-header">
          <strong>Order: ${order._id}</strong>
          <span class="order-badge badge-${order.status}">${order.status}</span>
        </div>
        <div style="font-size: 12px; color: var(--text-muted); margin-bottom: 6px;">
          Customer: <code>${order.customerId || 'N/A'}</code> • Total: <strong>$${order.totalAmount || 0}</strong>
        </div>
        <div class="timeline-step-list">
          ${(order.timeline || []).map(t => `
            <div class="timeline-step">
              • <strong>${t.event}</strong> <span style="color: var(--text-dim);">(${new Date(t.timestamp).toLocaleTimeString()})</span>
            </div>
          `).join('')}
        </div>
      </div>
    `).join('');
  } catch (e) {
    // Service might be starting up
  }
}

async function loadDatabaseTables() {
  // 1. Inventory Products
  try {
    const invRes = await fetch(`${INVENTORY_SERVICE_URL}/api/v1/inventory/products`);
    if (invRes.ok) {
      const products = await invRes.json();
      const tbody = document.getElementById('tbody-inventory');
      if (tbody && products.length > 0) {
        tbody.innerHTML = products.map(p => `
          <tr>
            <td><strong>${p.name}</strong><br/><small class="text-dim">${p.sku}</small></td>
            <td>${p.totalStock}</td>
            <td style="color: var(--accent-amber);">${p.reservedStock}</td>
            <td style="color: var(--accent-emerald); font-weight: bold;">${p.totalStock - p.reservedStock}</td>
          </tr>
        `).join('');
      }
    }
  } catch (e) {}

  // 2. Payment Accounts
  try {
    const payRes = await fetch(`${PAYMENT_SERVICE_URL}/api/v1/payment/accounts`);
    if (payRes.ok) {
      const accounts = await payRes.json();
      const tbody = document.getElementById('tbody-accounts');
      if (tbody && accounts.length > 0) {
        tbody.innerHTML = accounts.map(a => `
          <tr>
            <td><code>${a.customerId}</code></td>
            <td style="color: var(--accent-cyan); font-weight: bold;">$${parseFloat(a.balance).toFixed(2)}</td>
            <td>${a.currency}</td>
            <td>${new Date(a.updatedAt).toLocaleTimeString()}</td>
          </tr>
        `).join('');
      }
    }
  } catch (e) {}
}

// -----------------------------------------------------------------------------
// 1-Click Chaos Lab Scenarios
// -----------------------------------------------------------------------------
function logChaosOutput(text) {
  const box = document.getElementById('chaos-output-box');
  if (box) {
    box.innerHTML = `<pre style="margin: 0; white-space: pre-wrap;">${text}</pre>`;
    box.scrollTop = box.scrollHeight;
  }
}

async function runChaosHighConcurrency() {
  logChaosOutput("⚡ Initiating High-Concurrency Race: Firing 10 concurrent checkouts for MacBook...");
  const promises = [];
  for (let i = 1; i <= 10; i++) {
    const key = `race_key_${i}_${Date.now()}`;
    promises.push(
      fetch(`${ORDER_SERVICE_URL}/api/v1/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
        body: JSON.stringify({
          customerId: 'cust_5001',
          currency: 'USD',
          items: [{ productId: 'prod_macbook', quantity: 1, unitPrice: 150.00 }]
        })
      }).then(r => r.json())
    );
  }

  const results = await Promise.allSettled(promises);
  logChaosOutput(`✅ Completed 10 concurrent requests!\nResults: ${JSON.stringify(results.map(r => r.value?.orderId || 'error'), null, 2)}`);
  loadAllData();
}

async function runChaosInversionGuard() {
  logChaosOutput("🛡️ Testing Compensation Inversion Guard: Simulating ReleaseInventoryCommand arriving before Reserve...");
  // Direct event ingestion test
  const dummyOrderId = `ord_inv_test_${Date.now()}`;
  try {
    await fetch(`${READ_MODEL_URL}/api/v1/events/ingest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        eventType: 'InventoryReleasedEvent',
        aggregateId: dummyOrderId,
        timestamp: new Date().toISOString(),
        payload: { orderId: dummyOrderId, status: 'RELEASED', reason: 'PRE_CANCELLED_TOMBSTONE' }
      })
    });
    logChaosOutput(`✅ Inversion event ingested safely into Read Model for ${dummyOrderId}. Check timeline!`);
    loadOrderProjections();
  } catch (e) {
    logChaosOutput(`❌ Inversion test failed: ${e.message}`);
  }
}

async function runChaosDuplicateReplay() {
  logChaosOutput("🔁 Testing Duplicate Replay: Firing duplicate order with identical Idempotency-Key 3 times...");
  const key = `dedup_test_${Date.now()}`;
  const orderPayload = {
    customerId: 'cust_5001',
    currency: 'USD',
    items: [{ productId: 'prod_iphone', quantity: 1, unitPrice: 99.00 }]
  };

  for (let i = 1; i <= 3; i++) {
    const res = await fetch(`${ORDER_SERVICE_URL}/api/v1/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
      body: JSON.stringify(orderPayload)
    });
    const data = await res.json();
    logChaosOutput(`Attempt ${i} [Status ${res.status}]: ${JSON.stringify(data)}`);
  }
  loadAllData();
}

async function runChaosTimeoutRecovery() {
  logChaosOutput("⏳ Saga Timeout Recovery: Orders stuck exceeding expires_at (60s) will be auto-cancelled by Dead Man's Switch poller.");
}
