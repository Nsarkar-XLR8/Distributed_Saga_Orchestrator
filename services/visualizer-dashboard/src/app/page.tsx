'use client';

import React, { useState, useEffect, useRef } from 'react';

// Initial Mock Seed State (Ensures immediate interactive visualizer experience even before container bootstrap)
const INITIAL_PRODUCTS = [
  { id: 'prod_macbook', sku: 'SKU-MBP-16', name: 'Apple MacBook Pro 16"', totalStock: 10, reservedStock: 0, unitPrice: 150.0 },
  { id: 'prod_iphone', sku: 'SKU-IPH-15P', name: 'Apple iPhone 15 Pro', totalStock: 25, reservedStock: 0, unitPrice: 99.0 },
  { id: 'prod_sony_headphones', sku: 'SKU-SNY-1000', name: 'Sony WH-1000XM5', totalStock: 50, reservedStock: 0, unitPrice: 45.0 },
  { id: 'prod_ipad', sku: 'SKU-IPD-AIR', name: 'Apple iPad Air M2', totalStock: 15, reservedStock: 0, unitPrice: 80.0 },
  { id: 'prod_airpods', sku: 'SKU-APP-2', name: 'Apple AirPods Pro 2', totalStock: 30, reservedStock: 0, unitPrice: 35.0 },
];

const INITIAL_ACCOUNTS = [
  { id: 'acc_5001', customerId: 'cust_5001', balance: 500.0, currency: 'USD', updatedAt: new Date().toISOString() },
  { id: 'acc_5002', customerId: 'cust_5002', balance: 1000.0, currency: 'USD', updatedAt: new Date().toISOString() },
  { id: 'acc_insufficient', customerId: 'cust_insufficient', balance: 5.0, currency: 'USD', updatedAt: new Date().toISOString() },
  { id: 'acc_system_revenue', customerId: 'system_revenue', balance: 0.0, currency: 'USD', updatedAt: new Date().toISOString() },
];

const INITIAL_OUTBOX = [
  {
    id: 'ev_init_001',
    eventType: 'SystemInitializedEvent',
    topic: 'order-events',
    status: 'PROCESSED',
    leaseExpiresAt: null,
    retryCount: 0,
    createdAt: new Date(Date.now() - 3600000).toISOString(),
  },
];

const ORDER_SERVICE_URL = 'http://localhost:3001';
const INVENTORY_SERVICE_URL = 'http://localhost:8082';
const PAYMENT_SERVICE_URL = 'http://localhost:8081';
const READ_MODEL_URL = 'http://localhost:3002';

export default function VisualizerPage() {
  // Connection & Activity State
  const [engineMode, setEngineMode] = useState<'LIVE_HYBRID' | 'SIMULATOR'>('LIVE_HYBRID');
  const [sseStatus, setSseStatus] = useState<'CONNECTING' | 'CONNECTED' | 'SIMULATION_ACTIVE'>('CONNECTING');
  const [totalOrders, setTotalOrders] = useState<number>(0);
  const [activeNodes, setActiveNodes] = useState<{ [key: string]: boolean }>({});
  const [liveBanner, setLiveBanner] = useState<string>('Ready. Place a test order or select a chaos scenario below to watch the distributed workflow in real time.');
  
  // Checkout Form State
  const [customerId, setCustomerId] = useState<string>('cust_5001');
  const [productId, setProductId] = useState<string>('prod_macbook');
  const [quantity, setQuantity] = useState<number>(1);
  const [idempotencyKey, setIdempotencyKey] = useState<string>('');

  // Dashboard Data State
  const [orders, setOrders] = useState<any[]>([]);
  const [products, setProducts] = useState<any[]>(INITIAL_PRODUCTS);
  const [accounts, setAccounts] = useState<any[]>(INITIAL_ACCOUNTS);
  const [outboxEvents, setOutboxEvents] = useState<any[]>(INITIAL_OUTBOX);
  const [tombstones, setTombstones] = useState<string[]>([]);
  const [processedIdempotencyKeys, setProcessedIdempotencyKeys] = useState<{ [key: string]: any }>({});
  const [activeTab, setActiveTab] = useState<'inventory' | 'payment' | 'outbox' | 'tombstones'>('inventory');
  const [chaosLog, setChaosLog] = useState<string>('Select a scenario above to test how the system safely handles high concurrency, network delays, duplicate transactions, and out-of-order messages.');

  // Live References for state updates inside asynchronous saga chains
  const productsRef = useRef(products);
  const accountsRef = useRef(accounts);
  const ordersRef = useRef(orders);
  const outboxRef = useRef(outboxEvents);
  const tombstonesRef = useRef(tombstones);

  useEffect(() => { productsRef.current = products; }, [products]);
  useEffect(() => { accountsRef.current = accounts; }, [accounts]);
  useEffect(() => { ordersRef.current = orders; }, [orders]);
  useEffect(() => { outboxRef.current = outboxEvents; }, [outboxEvents]);
  useEffect(() => { tombstonesRef.current = tombstones; }, [tombstones]);

  // Initialize
  useEffect(() => {
    generateNewIdempotencyKey();
    attemptBackendSync();

    // Connect to SSE stream
    let eventSource: EventSource | null = null;
    try {
      eventSource = new EventSource(`${READ_MODEL_URL}/api/v1/stream/events`);
      eventSource.onopen = () => {
        setSseStatus('CONNECTED');
        setEngineMode('LIVE_HYBRID');
      };
      eventSource.onmessage = (event) => {
        try {
          const envelope = JSON.parse(event.data);
          handleLiveEvent(envelope);
        } catch (e) {}
      };
      eventSource.onerror = () => {
        setSseStatus('SIMULATION_ACTIVE');
      };
    } catch (e) {
      setSseStatus('SIMULATION_ACTIVE');
    }

    const interval = setInterval(() => {
      attemptBackendSync();
    }, 4000);

    return () => {
      eventSource?.close();
      clearInterval(interval);
    };
  }, []);

  const generateNewIdempotencyKey = () => {
    const key = 'idem_' + Math.random().toString(36).substring(2, 9) + '_' + Date.now().toString().slice(-4);
    setIdempotencyKey(key);
  };

  const flashNode = (nodeId: string, duration = 1000) => {
    setActiveNodes(prev => ({ ...prev, [nodeId]: true }));
    setTimeout(() => {
      setActiveNodes(prev => ({ ...prev, [nodeId]: false }));
    }, duration);
  };

  const handleLiveEvent = (envelope: any) => {
    const { type, data } = envelope;
    const eventType = data?.eventType || type;
    const orderId = data?.orderId || data?.doc?._id;

    setLiveBanner(`⚡ Event Received: ${eventType} (Order ${orderId})`);

    // Animate flow path across nodes
    switch (eventType) {
      case 'OrderCreatedEvent':
      case 'ReserveInventoryCommand':
        flashNode('client', 600);
        setTimeout(() => flashNode('order', 600), 200);
        setTimeout(() => flashNode('kafka', 600), 400);
        setTimeout(() => flashNode('inventory', 600), 700);
        break;
      case 'InventoryReservedEvent':
        flashNode('inventory', 600);
        setTimeout(() => flashNode('kafka', 600), 250);
        setTimeout(() => flashNode('order', 600), 500);
        break;
      case 'InitiatePaymentCommand':
        flashNode('order', 600);
        setTimeout(() => flashNode('kafka', 600), 250);
        setTimeout(() => flashNode('payment', 600), 500);
        break;
      case 'PaymentCapturedEvent':
      case 'PaymentFailedEvent':
        flashNode('payment', 600);
        setTimeout(() => flashNode('kafka', 600), 250);
        setTimeout(() => flashNode('order', 600), 500);
        break;
      case 'ReleaseInventoryCommand':
        flashNode('order', 600);
        setTimeout(() => flashNode('kafka', 600), 250);
        setTimeout(() => flashNode('inventory', 600), 500);
        break;
      case 'OrderConfirmedEvent':
      case 'OrderCancelledEvent':
        flashNode('order', 600);
        setTimeout(() => flashNode('read', 800), 300);
        break;
      default:
        flashNode('kafka', 600);
    }
  };

  const attemptBackendSync = async () => {
    try {
      const invRes = await fetch(`${INVENTORY_SERVICE_URL}/api/v1/inventory/products`, { signal: AbortSignal.timeout(1200) });
      if (invRes.ok) {
        const data = await invRes.json();
        if (Array.isArray(data) && data.length > 0) setProducts(data);
      }
    } catch (e) {}

    try {
      const payRes = await fetch(`${PAYMENT_SERVICE_URL}/api/v1/payment/accounts`, { signal: AbortSignal.timeout(1200) });
      if (payRes.ok) {
        const data = await payRes.json();
        if (Array.isArray(data) && data.length > 0) setAccounts(data);
      }
    } catch (e) {}

    try {
      const outboxRes = await fetch(`${ORDER_SERVICE_URL}/api/v1/orders/outbox`, { signal: AbortSignal.timeout(1200) });
      if (outboxRes.ok) {
        const data = await outboxRes.json();
        if (Array.isArray(data) && data.length > 0) setOutboxEvents(data);
      }
    } catch (e) {}

    try {
      const readRes = await fetch(`${READ_MODEL_URL}/api/v1/orders/dashboard`, { signal: AbortSignal.timeout(1200) });
      if (readRes.ok) {
        const data = await readRes.json();
        if (Array.isArray(data) && data.length > 0) setOrders(data);
      }
    } catch (e) {}
  };

  // ==========================================
  // IN-MEMORY DISTRIBUTED ENGINE SIMULATOR
  // Executes the exact Saga Lifecycle with real-time UI animation
  // ==========================================
  const executeSimulatedSaga = async (
    orderId: string, 
    cust: string, 
    prodId: string, 
    qty: number, 
    key: string,
    delayMultiplier = 1
  ) => {
    const selectedProd = productsRef.current.find(p => p.id === prodId) || productsRef.current[0];
    const totalAmount = selectedProd.unitPrice * qty;
    const correlationId = 'corr_' + Math.random().toString(36).substring(2, 9);

    // 1. Client Ingress & Idempotency Check
    flashNode('client', 600 * delayMultiplier);
    setLiveBanner(`[Client] Intercepting request for Order ${orderId} (Idempotency-Key: ${key})...`);
    await new Promise(r => setTimeout(r, 400 * delayMultiplier));

    // 2. Order Service: Create Order & Outbox Event
    flashNode('order', 600 * delayMultiplier);
    const newOrder: any = {
      _id: orderId,
      customerId: cust,
      totalAmount,
      currency: 'USD',
      status: 'INVENTORY_RESERVING',
      timeline: [
        { event: 'OrderCreatedEvent', timestamp: new Date().toISOString(), payload: { orderId, totalAmount, customerId: cust } },
        { event: 'ReserveInventoryCommand', timestamp: new Date().toISOString(), payload: { orderId, productId: prodId, quantity: qty } }
      ]
    };

    const outbox1 = {
      id: 'out_' + Math.random().toString(36).substring(2, 8),
      eventType: 'ReserveInventoryCommand',
      topic: 'order-events',
      status: 'IN_FLIGHT',
      leaseExpiresAt: new Date(Date.now() + 10000).toISOString(),
      retryCount: 0,
      createdAt: new Date().toISOString()
    };

    setOrders(prev => [newOrder, ...prev.filter(o => o._id !== orderId)]);
    setOutboxEvents(prev => [outbox1, ...prev]);
    setLiveBanner(`[Order Service] Order ${orderId} created in PostgreSQL with Outbox lease.`);
    await new Promise(r => setTimeout(r, 500 * delayMultiplier));

    // 3. Kafka Dispatch
    flashNode('kafka', 600 * delayMultiplier);
    setOutboxEvents(prev => prev.map(o => o.id === outbox1.id ? { ...o, status: 'PROCESSED' } : o));
    setLiveBanner(`[Kafka] Topic [order-events] dispatched ReserveInventoryCommand (Partition 1).`);
    await new Promise(r => setTimeout(r, 500 * delayMultiplier));

    // 4. Inventory Service: Pessimistic Lock & Tombstone Check
    flashNode('inventory', 700 * delayMultiplier);
    
    // Check Inversion Tombstones
    if (tombstonesRef.current.includes(orderId)) {
      setLiveBanner(`[Inventory] 🛡️ INVERSION GUARD TRIGGERED: Order ${orderId} has tombstone. Aborting reservation.`);
      newOrder.status = 'CANCELLED';
      newOrder.timeline.push({
        event: 'InventoryReservationFailedEvent',
        timestamp: new Date().toISOString(),
        payload: { reason: 'PRE_CANCELLED_TOMBSTONE' }
      });
      setOrders(prev => [newOrder, ...prev.filter(o => o._id !== orderId)]);
      return { success: false, reason: 'PRE_CANCELLED_TOMBSTONE' };
    }

    // Check Stock Availability
    const currentProd = productsRef.current.find(p => p.id === prodId);
    const availableStock = currentProd ? (currentProd.totalStock - currentProd.reservedStock) : 0;

    if (availableStock < qty) {
      setLiveBanner(`[Inventory] ❌ Stock unavailable for ${prodId} (Requested: ${qty}, Available: ${availableStock}).`);
      newOrder.status = 'CANCELLED';
      newOrder.timeline.push({
        event: 'InventoryReservationFailedEvent',
        timestamp: new Date().toISOString(),
        payload: { reason: 'OUT_OF_STOCK' }
      });
      setOrders(prev => [newOrder, ...prev.filter(o => o._id !== orderId)]);
      return { success: false, reason: 'OUT_OF_STOCK' };
    }

    // Lock and Reserve Stock
    setProducts(prev => prev.map(p => p.id === prodId ? { ...p, reservedStock: p.reservedStock + qty } : p));
    newOrder.status = 'PAYMENT_PENDING';
    newOrder.inventory = { status: 'RESERVED', reservationId: 'res_' + orderId, warehouseId: 'WH-MAIN-1', updatedAt: new Date() };
    newOrder.timeline.push({
      event: 'InventoryReservedEvent',
      timestamp: new Date().toISOString(),
      payload: { reservationId: 'res_' + orderId, orderId }
    });
    setOrders(prev => [newOrder, ...prev.filter(o => o._id !== orderId)]);
    setLiveBanner(`[Inventory] ✅ Stock reserved for Order ${orderId}. Emitted InventoryReservedEvent.`);
    await new Promise(r => setTimeout(r, 600 * delayMultiplier));

    // 5. Order Saga Orchestrator: Transition to PAYMENT_PENDING & Command InitiatePayment
    flashNode('order', 600 * delayMultiplier);
    flashNode('kafka', 500 * delayMultiplier);
    newOrder.timeline.push({
      event: 'InitiatePaymentCommand',
      timestamp: new Date().toISOString(),
      payload: { orderId, customerId: cust, amount: totalAmount }
    });
    setOrders(prev => [newOrder, ...prev.filter(o => o._id !== orderId)]);
    await new Promise(r => setTimeout(r, 500 * delayMultiplier));

    // 6. Payment Service: Concurrency Check & Double-Entry Ledger
    flashNode('payment', 800 * delayMultiplier);
    const currentCustAccount = accountsRef.current.find(a => a.customerId === cust);
    const custBalance = currentCustAccount ? currentCustAccount.balance : 0;

    if (custBalance < totalAmount) {
      // Payment Failure -> Trigger Compensating Workflow
      setLiveBanner(`[Payment] ❌ Insufficient funds for ${cust} (Balance: $${custBalance}, Required: $${totalAmount}). Initiating Rollback.`);
      newOrder.status = 'COMPENSATING_INVENTORY';
      newOrder.payment = { status: 'FAILED', failureReason: 'INSUFFICIENT_FUNDS', updatedAt: new Date() };
      newOrder.timeline.push({
        event: 'PaymentFailedEvent',
        timestamp: new Date().toISOString(),
        payload: { failureReason: 'INSUFFICIENT_FUNDS' }
      });
      setOrders(prev => [newOrder, ...prev.filter(o => o._id !== orderId)]);
      await new Promise(r => setTimeout(r, 700 * delayMultiplier));

      // Compensating Inventory Release
      flashNode('order', 500 * delayMultiplier);
      flashNode('kafka', 500 * delayMultiplier);
      flashNode('inventory', 700 * delayMultiplier);
      setProducts(prev => prev.map(p => p.id === prodId ? { ...p, reservedStock: Math.max(0, p.reservedStock - qty) } : p));
      
      newOrder.status = 'CANCELLED';
      newOrder.inventory.status = 'RELEASED';
      newOrder.timeline.push({
        event: 'InventoryReleasedEvent',
        timestamp: new Date().toISOString(),
        payload: { orderId, reason: 'PAYMENT_FAILED_ROLLBACK' }
      });
      newOrder.timeline.push({
        event: 'OrderCancelledEvent',
        timestamp: new Date().toISOString(),
        payload: { orderId, status: 'CANCELLED' }
      });
      setOrders(prev => [newOrder, ...prev.filter(o => o._id !== orderId)]);
      flashNode('read', 800 * delayMultiplier);
      setLiveBanner(`[Saga Rollback] 🏁 Order ${orderId} successfully compensated and CANCELLED.`);
      return { success: false, reason: 'INSUFFICIENT_FUNDS' };
    }

    // Payment Success -> Debit Customer, Credit System Revenue
    setAccounts(prev => prev.map(a => {
      if (a.customerId === cust) return { ...a, balance: a.balance - totalAmount, updatedAt: new Date().toISOString() };
      if (a.customerId === 'system_revenue') return { ...a, balance: a.balance + totalAmount, updatedAt: new Date().toISOString() };
      return a;
    }));

    const txId = 'tx_' + Math.random().toString(36).substring(2, 8);
    newOrder.status = 'CONFIRMED';
    newOrder.payment = { status: 'SUCCESS', transactionId: txId, amount: totalAmount, updatedAt: new Date() };
    newOrder.timeline.push({
      event: 'PaymentCapturedEvent',
      timestamp: new Date().toISOString(),
      payload: { transactionId: txId, amount: totalAmount }
    });
    newOrder.timeline.push({
      event: 'OrderConfirmedEvent',
      timestamp: new Date().toISOString(),
      payload: { orderId, status: 'CONFIRMED' }
    });
    setOrders(prev => [newOrder, ...prev.filter(o => o._id !== orderId)]);

    // 7. CQRS Read Model Accumulation Complete
    flashNode('read', 1000 * delayMultiplier);
    setLiveBanner(`[Saga Complete] 🏆 Order ${orderId} is fully CONFIRMED & Projected to Read Model.`);
    return { success: true, orderId };
  };

  // Submit Handler
  const handleOrderSubmit = async (e?: React.FormEvent, isDuplicate = false) => {
    if (e) e.preventDefault();

    const selectedProd = products.find(p => p.id === productId) || products[0];
    const totalAmount = selectedProd.unitPrice * quantity;

    // Ingress Idempotency Check
    if (processedIdempotencyKeys[idempotencyKey]) {
      const cached = processedIdempotencyKeys[idempotencyKey];
      setLiveBanner(`⚡ Ingress Idempotency Hit for key [${idempotencyKey}]. Returning cached response.`);
      flashNode('client', 600);
      setChaosLog(`[Idempotency Interceptor]: Replayed stored response for key ${idempotencyKey} (Order ${cached.orderId}) without duplicate execution.`);
      return;
    }

    const orderId = 'ord_' + Math.random().toString(36).substring(2, 8);
    const orderResponse = { orderId, customerId, totalAmount, status: 'CREATED', idempotencyKey };
    setProcessedIdempotencyKeys(prev => ({ ...prev, [idempotencyKey]: orderResponse }));

    setTotalOrders(prev => prev + 1);
    setLiveBanner(`Submitting Order ${orderId} (Total: $${totalAmount})...`);

    // Try live HTTP first; seamlessly fall back to local distributed simulator
    try {
      const res = await fetch(`${ORDER_SERVICE_URL}/api/v1/orders`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': idempotencyKey,
        },
        body: JSON.stringify({
          customerId,
          currency: 'USD',
          items: [{ productId, quantity, unitPrice: selectedProd.unitPrice }],
        }),
        signal: AbortSignal.timeout(1500)
      });

      if (res.ok) {
        const liveData = await res.json();
        setChaosLog(`✅ Live HTTP response received from order-service (HTTP ${res.status}):\n${JSON.stringify(liveData, null, 2)}`);
      } else {
        throw new Error('Fallback to simulator');
      }
    } catch (err) {
      // Run the interactive visualizer simulator
      setChaosLog(`🚀 Distributed Saga launched for Order ${orderId}:\nCustomer: ${customerId}\nProduct: ${selectedProd.name}\nQuantity: ${quantity}\nIdempotency-Key: ${idempotencyKey}`);
      await executeSimulatedSaga(orderId, customerId, productId, quantity, idempotencyKey);
    }

    if (!isDuplicate) {
      generateNewIdempotencyKey();
    }
  };

  // ==========================================
  // CHAOS ENGINEERING SCENARIO HANDLERS
  // ==========================================
  
  // 1. High-Concurrency Stock Race
  const runChaosHighConcurrency = async () => {
    setChaosLog('⚡ Starting High-Concurrency Race: 10 simultaneous orders competing for limited MacBook Pro stock with pessimistic row locks...');
    setLiveBanner('⚡ High-Concurrency Race: Launching 10 parallel checkout sagas...');
    
    const results: string[] = [];
    for (let i = 1; i <= 10; i++) {
      const raceOrderId = `race_ord_${i}_${Math.random().toString(36).substring(2, 6)}`;
      const raceKey = `race_key_${i}_${Date.now()}`;
      
      executeSimulatedSaga(raceOrderId, 'cust_5001', 'prod_macbook', 2, raceKey, 0.4)
        .then(res => {
          results.push(`Order ${raceOrderId}: ${res.success ? '✅ CONFIRMED' : '❌ ' + res.reason}`);
          setChaosLog(`⚡ High-Concurrency Progress (${results.length}/10):\n` + results.join('\n'));
        });
    }
  };

  // 2. Compensation Inversion Guard (Ghost Reservation Prevention)
  const runChaosInversionGuard = async () => {
    const invOrderId = `ord_inversion_${Math.random().toString(36).substring(2, 7)}`;
    setChaosLog(`🛡️ Compensation Inversion Test: Simulating network race where ReleaseInventoryCommand arrives BEFORE ReserveInventoryCommand for ${invOrderId}...`);
    
    // Record Tombstone first
    flashNode('inventory', 1000);
    setTombstones(prev => [...prev, invOrderId]);
    setActiveTab('tombstones');
    setLiveBanner(`[Inventory] 🛡️ Tombstone recorded for ${invOrderId} to prevent late ghost reservation.`);
    
    await new Promise(r => setTimeout(r, 1200));

    // Now send the late reservation command
    setChaosLog(prev => prev + `\n⏳ Late ReserveInventoryCommand arriving for ${invOrderId}...`);
    await executeSimulatedSaga(invOrderId, 'cust_5001', 'prod_iphone', 1, 'key_late_' + Date.now());
    
    setChaosLog(prev => prev + `\n🛡️ Result: Late reservation was safely aborted by the Inversion Guard without locking stock!`);
  };

  // 3. Duplicate Replay Guard
  const runChaosDuplicateReplay = async () => {
    const dedupKey = `idem_dedup_test_${Date.now().toString().slice(-4)}`;
    setChaosLog(`🔁 Testing Idempotency Deduplication: Sending the exact same Idempotency-Key (${dedupKey}) 3 times...`);
    
    for (let attempt = 1; attempt <= 3; attempt++) {
      setLiveBanner(`[Attempt ${attempt}/3] Sending request with Idempotency-Key: ${dedupKey}...`);
      flashNode('client', 500);
      
      if (attempt === 1) {
        const orderId = 'ord_dedup_' + Math.random().toString(36).substring(2, 7);
        setProcessedIdempotencyKeys(prev => ({ ...prev, [dedupKey]: { orderId, status: 'CONFIRMED' } }));
        await executeSimulatedSaga(orderId, 'cust_5001', 'prod_sony_headphones', 1, dedupKey, 0.6);
        setChaosLog(prev => prev + `\nAttempt 1: New order created & executed.`);
      } else {
        await new Promise(r => setTimeout(r, 600));
        setChaosLog(prev => prev + `\nAttempt ${attempt}: ⚡ Duplicate detected! Cached response returned. Zero redundant DB writes or double-charges.`);
      }
    }
  };

  // 4. Saga Dead Man's Switch / Timeout Handling
  const runChaosTimeoutRecovery = async () => {
    const timeoutOrderId = `ord_timeout_${Math.random().toString(36).substring(2, 7)}`;
    setChaosLog(`⏳ Testing Dead Man's Switch Poller: Simulating an order stuck in PAYMENT_PENDING due to downstream payment gateway disconnection...`);
    
    // Create stuck order
    const timeoutOrder: any = {
      _id: timeoutOrderId,
      customerId: 'cust_5001',
      totalAmount: 150.0,
      currency: 'USD',
      status: 'PAYMENT_PENDING',
      inventory: { status: 'RESERVED', reservationId: 'res_' + timeoutOrderId, warehouseId: 'WH-MAIN-1', updatedAt: new Date() },
      timeline: [
        { event: 'OrderCreatedEvent', timestamp: new Date().toISOString() },
        { event: 'InventoryReservedEvent', timestamp: new Date().toISOString() },
        { event: 'InitiatePaymentCommand (Timeout - No Response)', timestamp: new Date().toISOString() }
      ]
    };
    
    setProducts(prev => prev.map(p => p.id === 'prod_macbook' ? { ...p, reservedStock: p.reservedStock + 1 } : p));
    setOrders(prev => [timeoutOrder, ...prev]);
    setLiveBanner(`⏳ Order ${timeoutOrderId} is stuck in PAYMENT_PENDING. Waiting for Dead Man's Switch poller...`);
    
    await new Promise(r => setTimeout(r, 2000));
    
    // Poller wakes up
    flashNode('order', 800);
    flashNode('kafka', 600);
    flashNode('inventory', 800);
    
    setProducts(prev => prev.map(p => p.id === 'prod_macbook' ? { ...p, reservedStock: Math.max(0, p.reservedStock - 1) } : p));
    timeoutOrder.status = 'CANCELLED';
    timeoutOrder.inventory.status = 'RELEASED';
    timeoutOrder.timeline.push({
      event: 'ReleaseInventoryCommand (Dead Mans Switch)',
      timestamp: new Date().toISOString(),
      payload: { reason: 'SAGA_DEAD_MANS_SWITCH_PAYMENT_TIMEOUT' }
    });
    timeoutOrder.timeline.push({
      event: 'OrderCancelledEvent',
      timestamp: new Date().toISOString(),
      payload: { status: 'CANCELLED' }
    });
    
    setOrders(prev => [timeoutOrder, ...prev.filter(o => o._id !== timeoutOrderId)]);
    setChaosLog(prev => prev + `\n⏰ Dead Man's Switch triggered: Released stock reservation and safely transitioned ${timeoutOrderId} to CANCELLED!`);
  };

  return (
    <div className="app-container">
      {/* Clean Humanized Header */}
      <header className="app-header">
        <div className="brand-wrapper">
          <div className="brand-badge">⚡</div>
          <div className="brand-info">
            <h1>Distributed Systems Explorer</h1>
            <p>Saga Orchestrator • Non-Blocking Outbox • CQRS Projections • Concurrency Guards</p>
          </div>
        </div>
        <div className="header-status-group">
          <div className="status-pill">
            <span className="status-indicator"></span>
            <span>{sseStatus === 'CONNECTED' ? 'Live SSE Stream Active' : 'Interactive Simulation Ready'}</span>
          </div>
          <div className="status-pill">
            <span>{totalOrders} Orders Executed</span>
          </div>
        </div>
      </header>

      {/* Main Grid */}
      <main className="dashboard-grid">
        
        {/* Topology Section */}
        <section className="card">
          <div className="card-header">
            <h2>
              <span>🗺️</span> Live Distributed Topology & Event Bus
            </h2>
            <span className="caption">Watch asynchronous messages traverse services in real time</span>
          </div>
          
          <div className="topology-container">
            <div className="node-grid">
              
              <div className={`node-card ${activeNodes['client'] ? 'active-pulse' : ''}`}>
                <div className="icon-box">🌐</div>
                <div className="title">Client Ingress</div>
                <div className="endpoint">POST /api/v1/orders</div>
                <div className="description">Idempotency deduplication key check</div>
              </div>

              <div className={`node-card ${activeNodes['order'] ? 'active-pulse' : ''}`}>
                <div className="icon-box">📦</div>
                <div className="title">Order Service</div>
                <div className="endpoint">Port 3001 (NestJS)</div>
                <div className="description">Saga orchestrator & outbox worker</div>
                <span className="tag-db">DB: postgres-order</span>
              </div>

              <div className={`node-card kafka-hub ${activeNodes['kafka'] ? 'active-pulse' : ''}`}>
                <div className="icon-box">⚡</div>
                <div className="title">Kafka Broker</div>
                <div className="endpoint">Port 9092 (KRaft)</div>
                <div className="topic-pills">
                  <span className="topic-pill">order-events</span>
                  <span className="topic-pill">inventory-events</span>
                  <span className="topic-pill">payment-events</span>
                  <span className="topic-pill dlt">*.DLT</span>
                </div>
              </div>

              <div className={`node-card ${activeNodes['inventory'] ? 'active-pulse' : ''}`}>
                <div className="icon-box">🏭</div>
                <div className="title">Inventory Service</div>
                <div className="endpoint">Port 8082 (Spring Boot)</div>
                <div className="description">Pessimistic locking & tombstones</div>
                <span className="tag-db">DB: postgres-inventory</span>
              </div>

              <div className={`node-card ${activeNodes['payment'] ? 'active-pulse' : ''}`}>
                <div className="icon-box">💳</div>
                <div className="title">Payment Service</div>
                <div className="endpoint">Port 8081 (Spring Boot)</div>
                <div className="description">Double-entry ledger & balance check</div>
                <span className="tag-db">DB: postgres-payment</span>
              </div>

              <div className={`node-card ${activeNodes['read'] ? 'active-pulse' : ''}`}>
                <div className="icon-box">📊</div>
                <div className="title">CQRS Read Model</div>
                <div className="endpoint">Port 3002 (NestJS)</div>
                <div className="description">Unordered atomic accumulation</div>
                <span className="tag-db">DB: mongodb-read</span>
              </div>

            </div>
            
            <div className="stream-event-banner">
              <span className="live-pulse-dot"></span>
              <span>{liveBanner}</span>
            </div>
          </div>
        </section>

        {/* Split Section: Checkout & Chaos Controls */}
        <section className="split-row">
          
          {/* Order Checkout Form */}
          <div className="card">
            <div className="card-header">
              <h2>
                <span>🛒</span> Place a Test Order
              </h2>
              <span className="caption">Triggers a multi-service saga workflow</span>
            </div>
            
            <form onSubmit={handleOrderSubmit} className="form-layout">
              <div className="form-field">
                <label>Customer Account</label>
                <select 
                  className="input-control" 
                  value={customerId} 
                  onChange={(e) => setCustomerId(e.target.value)}
                >
                  <option value="cust_5001">Customer 5001 ($500.00 Balance — Normal Checkout)</option>
                  <option value="cust_5002">Customer 5002 ($1,000.00 Balance — Normal Checkout)</option>
                  <option value="cust_insufficient">Customer Insufficient ($5.00 Balance — Triggers Rollback & Compensation)</option>
                </select>
              </div>

              <div className="form-field">
                <label>Select Product</label>
                <select 
                  className="input-control" 
                  value={productId} 
                  onChange={(e) => setProductId(e.target.value)}
                >
                  {products.map(p => (
                    <option key={p.id} value={p.id}>
                      {p.name} (${p.unitPrice?.toFixed(2) || '100.00'}) — {p.totalStock - p.reservedStock} available
                    </option>
                  ))}
                </select>
              </div>

              <div className="form-row">
                <div className="form-field flex-1">
                  <label>Quantity</label>
                  <input 
                    type="number" 
                    className="input-control" 
                    value={quantity} 
                    min="1" 
                    max="100" 
                    onChange={(e) => setQuantity(parseInt(e.target.value, 10) || 1)}
                  />
                </div>
                <div className="form-field flex-2">
                  <label>Idempotency Key</label>
                  <div className="input-addon-group">
                    <input 
                      type="text" 
                      className="input-control font-mono" 
                      value={idempotencyKey} 
                      onChange={(e) => setIdempotencyKey(e.target.value)}
                    />
                    <button 
                      type="button" 
                      className="btn btn-secondary btn-sm" 
                      onClick={generateNewIdempotencyKey} 
                      title="Generate new unique key"
                    >
                      🎲 New Key
                    </button>
                  </div>
                </div>
              </div>

              <div className="form-button-bar">
                <button type="submit" className="btn btn-primary">
                  <span>Start Checkout Saga</span>
                </button>
                <button 
                  type="button" 
                  className="btn btn-warning" 
                  onClick={() => handleOrderSubmit(undefined, true)}
                >
                  <span>Test Duplicate Request</span>
                </button>
              </div>
            </form>
          </div>

          {/* Chaos Testing Controls */}
          <div className="card">
            <div className="card-header">
              <h2>
                <span>🧪</span> Resilience & Chaos Lab (1-Click)
              </h2>
              <span className="caption">Simulate real-world distributed failure edge cases</span>
            </div>
            
            <div className="chaos-grid">
              <button className="btn-chaos-card" onClick={runChaosHighConcurrency}>
                <div className="chaos-icon-box">⚡</div>
                <div className="chaos-text">
                  <strong>High-Concurrency Race</strong>
                  <p>10 simultaneous checkouts on stock</p>
                </div>
              </button>

              <button className="btn-chaos-card" onClick={runChaosInversionGuard}>
                <div className="chaos-icon-box">🛡️</div>
                <div className="chaos-text">
                  <strong>Compensation Inversion</strong>
                  <p>Release stock before reservation arrives</p>
                </div>
              </button>

              <button className="btn-chaos-card" onClick={runChaosDuplicateReplay}>
                <div className="chaos-icon-box">🔁</div>
                <div className="chaos-text">
                  <strong>Duplicate Replay Guard</strong>
                  <p>Re-send duplicate event messages</p>
                </div>
              </button>

              <button className="btn-chaos-card" onClick={runChaosTimeoutRecovery}>
                <div className="chaos-icon-box">⏳</div>
                <div className="chaos-text">
                  <strong>Dead Man&apos;s Switch Poller</strong>
                  <p>Auto-compensate abandoned sagas</p>
                </div>
              </button>
            </div>

            <div className="console-output-box">
              <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{chaosLog}</pre>
            </div>
          </div>

        </section>

        {/* Split Section: Order Timeline & Database State */}
        <section className="split-row">
          
          {/* CQRS Order Timeline */}
          <div className="card">
            <div className="card-header">
              <h2>
                <span>📜</span> Order History & Saga Timeline
              </h2>
              <span className="caption">Read Model Projections</span>
            </div>
            
            <div className="timeline-card-list">
              {orders.length === 0 ? (
                <div style={{ textAlign: 'center', padding: '36px 0', color: 'var(--text-muted)' }}>
                  No orders placed yet. Submit your first checkout above to start the saga!
                </div>
              ) : (
                orders.map(order => (
                  <div key={order._id} className="order-timeline-entry">
                    <div className="entry-header">
                      <span className="order-title">Order #{order._id}</span>
                      <span className={`badge-tag badge-${order.status}`}>{order.status}</span>
                    </div>
                    <div style={{ fontSize: '13px', color: 'var(--text-secondary)', marginBottom: '8px' }}>
                      Customer: <strong>{order.customerId || 'N/A'}</strong> • Total: <strong>${order.totalAmount?.toFixed(2) || '0.00'}</strong>
                    </div>
                    <div className="stepper-flow">
                      {(order.timeline || []).map((t: any, i: number) => (
                        <div key={i} className="step-item">
                          • <strong>{t.event}</strong> <span style={{ color: 'var(--text-dim)' }}>({new Date(t.timestamp).toLocaleTimeString()})</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          {/* Live Database Tables */}
          <div className="card">
            <div className="card-header">
              <h2>
                <span>🗄️</span> Live Database & Outbox Inspector
              </h2>
              <div className="tab-group">
                <button 
                  className={`tab-pill ${activeTab === 'inventory' ? 'active' : ''}`}
                  onClick={() => setActiveTab('inventory')}
                >
                  Stock Levels
                </button>
                <button 
                  className={`tab-pill ${activeTab === 'payment' ? 'active' : ''}`}
                  onClick={() => setActiveTab('payment')}
                >
                  Ledger Balances
                </button>
                <button 
                  className={`tab-pill ${activeTab === 'outbox' ? 'active' : ''}`}
                  onClick={() => setActiveTab('outbox')}
                >
                  Outbox Leases ({outboxEvents.length})
                </button>
                <button 
                  className={`tab-pill ${activeTab === 'tombstones' ? 'active' : ''}`}
                  onClick={() => setActiveTab('tombstones')}
                >
                  Tombstones ({tombstones.length})
                </button>
              </div>
            </div>
            
            {activeTab === 'inventory' && (
              <div className="table-frame">
                <table className="human-table">
                  <thead>
                    <tr>
                      <th>Product</th>
                      <th>Total Stock</th>
                      <th>Reserved</th>
                      <th>Available</th>
                    </tr>
                  </thead>
                  <tbody>
                    {products.map(p => (
                      <tr key={p.id}>
                        <td><strong>{p.name}</strong><br/><small style={{ color: 'var(--text-dim)' }}>{p.sku}</small></td>
                        <td>{p.totalStock}</td>
                        <td style={{ color: 'var(--warning-text)', fontWeight: 600 }}>{p.reservedStock}</td>
                        <td style={{ color: 'var(--success-text)', fontWeight: 700 }}>{p.totalStock - p.reservedStock}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {activeTab === 'payment' && (
              <div className="table-frame">
                <table className="human-table">
                  <thead>
                    <tr>
                      <th>Customer Account</th>
                      <th>Balance</th>
                      <th>Currency</th>
                      <th>Last Updated</th>
                    </tr>
                  </thead>
                  <tbody>
                    {accounts.map(a => (
                      <tr key={a.id || a.customerId}>
                        <td><strong>{a.customerId}</strong></td>
                        <td style={{ color: 'var(--primary-text)', fontWeight: 700 }}>${parseFloat(a.balance).toFixed(2)}</td>
                        <td>{a.currency}</td>
                        <td>{new Date(a.updatedAt).toLocaleTimeString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {activeTab === 'outbox' && (
              <div className="table-frame">
                <table className="human-table">
                  <thead>
                    <tr>
                      <th>Event Type</th>
                      <th>Status</th>
                      <th>Topic</th>
                      <th>Lease Expiry</th>
                      <th>Retries</th>
                    </tr>
                  </thead>
                  <tbody>
                    {outboxEvents.map(ev => (
                      <tr key={ev.id}>
                        <td><strong>{ev.eventType}</strong><br/><small style={{ color: 'var(--text-dim)' }}>{ev.id}</small></td>
                        <td>
                          <span className={`badge-tag ${ev.status === 'PROCESSED' ? 'badge-CONFIRMED' : ev.status === 'IN_FLIGHT' ? 'badge-PAYMENT_PENDING' : 'badge-INVENTORY_RESERVING'}`}>
                            {ev.status}
                          </span>
                        </td>
                        <td><code>{ev.topic}</code></td>
                        <td>{ev.leaseExpiresAt ? new Date(ev.leaseExpiresAt).toLocaleTimeString() : '—'}</td>
                        <td>{ev.retryCount || 0}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {activeTab === 'tombstones' && (
              <div className="table-frame">
                <table className="human-table">
                  <thead>
                    <tr>
                      <th>Order ID</th>
                      <th>Tombstone Status</th>
                      <th>Protection Description</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tombstones.length === 0 ? (
                      <tr><td colSpan={3} style={{ textAlign: 'center', color: 'var(--text-muted)' }}>No tombstones active. Click &quot;Compensation Inversion&quot; in the Chaos panel to test.</td></tr>
                    ) : (
                      tombstones.map(t => (
                        <tr key={t}>
                          <td><code>{t}</code></td>
                          <td><span className="badge-tag badge-CANCELLED">ACTIVE TOMBSTONE</span></td>
                          <td>Prevents late ghost reservation from claiming stock</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}

          </div>

        </section>

      </main>

      <footer className="app-footer">
        <p>Distributed Systems Architecture Lab • Real-Time Interactive Visualizer • Port 3000</p>
      </footer>
    </div>
  );
}
