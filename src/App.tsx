import React, { useState } from 'react';
import { 
  Server, Database, Network, ShieldCheck, Cpu, 
  Layers, CheckCircle2, Lock, ArrowRight, Activity, Terminal,
  RotateCcw, RefreshCw, Truck, Package, AlertCircle, Play, Check
} from 'lucide-react';

export default function App() {
  const [activeTab, setActiveTab] = useState<'overview' | 'saga' | 'services' | 'outbox' | 'compose'>('saga');
  const [selectedScenario, setSelectedScenario] = useState<'customer' | 'fulfillment' | 'shipping_timeout'>('shipping_timeout');
  const [sagaStep, setSagaStep] = useState<number>(0);
  const [isSimulating, setIsSimulating] = useState<boolean>(false);

  const microservices = [
    { name: 'API Gateway', port: 8000, tech: 'Spring Cloud Gateway', db: '-', role: 'Routing, Rate Limiting & SSL Termination' },
    { name: 'Auth Service', port: 8080, tech: 'Keycloak 24.0.2 + Spring Security', db: 'auth_db', role: 'Identity Provider, Dynamic RBAC, Customer & Staff Accounts' },
    { name: 'Order Service', port: 8081, tech: 'Java 21, Spring Boot 3.4.2, Redisson', db: 'orders_db', role: 'Dynamic Pricing, Redisson Distributed Lock, Transactional Outbox' },
    { name: 'Shipping & 3PL Service', port: 8082, tech: 'Spring Boot 3.4.2 + 3PL Adapters', db: 'shipping_db', role: '3PL Routing (Ahamove, GHTK), Cancellation Adapter & Redis Idempotency' },
    { name: 'Hub Fulfillment & Inventory', port: 8083, tech: 'Spring Boot 3.4.2 + Redisson MultiLock', db: 'fulfillment_db / inventory_db', role: 'Pharmacy Hub Stock Reservation & Release with Distributed Lock' },
    { name: 'Tracking & Webhook Service', port: 8084, tech: 'Spring Boot 3.4.2 + Redis Idempotency', db: 'tracking_db', role: 'Real-time 3PL Webhook Ingestion & Event Deduplication' },
    { name: 'Notification Service', port: 8085, tech: 'Spring Boot 3.4.2 + Kafka Listener', db: 'Redis cache', role: 'Omnichannel Push, Zalo ZNS, SMS, Email notifications' },
  ];

  const stack = [
    { label: 'Runtime & Core', val: 'Java 21 LTS | Spring Boot 3.4.2' },
    { label: 'Cloud Framework', val: 'Spring Cloud 2024.0.0' },
    { label: 'Database Migration', val: 'Liquibase 4.29.2 (ddl-auto: none)' },
    { label: 'Distributed Locking', val: 'Redis 7.2 + Redisson 3.42.0' },
    { label: 'Event Streaming', val: 'Apache Kafka 3.7.0 (KRaft Mode)' },
    { label: 'Storage Engine', val: 'PostgreSQL 16 (DB per Service)' },
    { label: 'IAM & Security', val: 'Keycloak 24.0.2 (OIDC/OAuth2)' },
  ];

  const customerSteps = [
    {
      step: 1,
      title: 'Bước 1: Khách hàng ấn Hủy đơn',
      service: 'Order Service (:8081)',
      action: 'Nhận POST /api/v1/orders/{id}/cancel -> Kiểm tra điều kiện -> Đổi trạng thái đơn sang CANCEL_REQUESTED -> Lưu sự kiện OrderCancelRequestedEvent vào bảng outbox_events trong CÙNG DB Transaction.',
      topic: 'order-cancellation-events',
      tech: 'ACID Transaction + Transactional Outbox + SKIP LOCKED poller',
      keyData: 'Status: CANCEL_REQUESTED | OutboxStatus: PENDING -> PUBLISHED',
    },
    {
      step: 2,
      title: 'Bước 2: Hủy chuyến vận chuyển 3PL',
      service: 'Shipping Service (:8082)',
      action: 'Lắng nghe Kafka Event -> Kiểm tra Idempotency bằng Redis Key (xcare:idempotency:shipping:cancel:{orderId}) -> Gọi API đối tác 3PL (Ahamove/GHTK) hủy chuyến -> Cập nhật Shipment = CANCELLED -> Bắn SHIPMENT_CANCELLED.',
      topic: 'shipping-cancellation-events',
      tech: 'Redis setIfAbsent (TTL 24h) + Ahamove/GHTK REST Adapter',
      keyData: 'Redis Key: xcare:idempotency:shipping:cancel:UUID (Status: COMPLETED)',
    },
    {
      step: 3,
      title: 'Bước 3: Hoàn trả tồn kho nhà thuốc',
      service: 'Inventory Service (:8083)',
      action: 'Lắng nghe SHIPMENT_CANCELLED -> Kiểm tra Idempotency Redis -> Sắp xếp SKU theo thứ tự từ điển -> Lấy Redisson MultiLock -> Hoàn trả tồn kho (Reserved -> Available) -> Giải phóng Lock -> Bắn INVENTORY_RELEASED.',
      topic: 'inventory-events',
      tech: 'Redisson Distributed MultiLock + Redis Idempotent Consumer',
      keyData: 'Lock Keys: xcare:lock:inventory:HUB-HN-001:SKU-xxx (RLock.unlock() safely)',
    },
    {
      step: 4,
      title: 'Bước 4: Đóng quy trình Saga',
      service: 'Order Service (:8081)',
      action: 'Lắng nghe INVENTORY_RELEASED -> Kiểm tra Idempotency đơn hàng -> Cập nhật trạng thái đơn thành CANCELLED_BY_CUSTOMER -> Ghi log kiểm toán & Đóng hoàn tất quy trình Saga Rollback.',
      topic: 'inventory-events -> Final Order State',
      tech: 'Kafka Listener + JPA Transaction + Customer Audit Log',
      keyData: 'Final Status: CANCELLED_BY_CUSTOMER (Saga Process Closed)',
    },
  ];

  const fulfillmentSteps = [
    {
      step: 1,
      title: 'Bước 1: Dược sĩ HỦY ĐÓNG GÓI',
      service: 'Fulfillment Service (:8083)',
      action: 'Dược sĩ phát hiện thuốc vỡ/ẩm mốc hoặc hết hàng tại kệ -> Gọi POST /api/v1/fulfillment/cancel-pack -> Lưu sự kiện FULFILLMENT_FAILED vào fulfillment_outbox (ACID Transaction) -> Outbox Publisher quét (SKIP LOCKED) bắn Kafka.',
      topic: 'fulfillment-events',
      tech: 'Transactional Outbox + PostgreSQL SKIP LOCKED + Spring Data JPA',
      keyData: 'Status: FULFILLMENT_FAILED | Outbox Table: fulfillment_outbox',
    },
    {
      step: 2,
      title: 'Bước 2: Hủy chuyến Ahamove 3PL',
      service: 'Shipping Service (:8082)',
      action: 'Lắng nghe topic fulfillment-events -> Kiểm tra Idempotency Redis -> Gọi API Ahamove cancelDeliveryOrder (lý do: "Kho hủy đóng gói do thuốc hỏng/hết hàng") -> Cập nhật Shipment = CANCELLED -> Bắn SHIPMENT_CANCELLED.',
      topic: 'shipping-events / shipping-cancellation-events',
      tech: 'Ahamove 3PL Adapter + Redis Idempotency Key (TTL 24h)',
      keyData: 'Carrier: AHAMOVE | ShipmentStatus: CANCELLED (3PL Order Terminated)',
    },
    {
      step: 3,
      title: 'Bước 3: Cách ly hàng & Giải phóng tồn ảo',
      service: 'Inventory Service (:8083)',
      action: 'Lắng nghe SHIPMENT_CANCELLED -> Kiểm tra Idempotency Redis -> Sort SKU alphabet -> Lấy Redisson MultiLock -> Trừ reserved_quantity, KHÔNG cộng vào available_quantity (cách ly thuốc hỏng) -> Bắn INVENTORY_RELEASED.',
      topic: 'inventory-events',
      tech: 'Redisson MultiLock + Quarantine Damaged Goods + Saga Event Emitter',
      keyData: 'SagaType: FULFILLMENT_FAILED_OUT_OF_STOCK | Deficit Quarantined',
    },
    {
      step: 4,
      title: 'Bước 4: Cập nhật CANCELLED_OUT_OF_STOCK',
      service: 'Order Service (:8081)',
      action: 'Lắng nghe INVENTORY_RELEASED -> Nhận diện sự cố kho (sagaType chứa FULFILLMENT) -> Cập nhật trạng thái đơn thành CANCELLED_OUT_OF_STOCK -> Ghi Prescription Audit Note -> Hoàn tất Saga Rollback!',
      topic: 'inventory-events -> Final Order State',
      tech: 'Kafka Idempotent Consumer + Order State Machine + Audit Trail',
      keyData: 'Final Status: CANCELLED_OUT_OF_STOCK (Saga Rollback Closed)',
    },
  ];

  const shippingTimeoutSteps = [
    {
      step: 1,
      title: 'Bước 3 (Shipping): 3PL Timeout & Resilience4j Fallback',
      service: 'Shipping Service (:8082)',
      action: 'Gọi API Ahamove/GHTK bị Timeout/500 -> Resilience4j Retry 3 lần thất bại -> Kích hoạt Fallback -> Ghi nhận Transactional Outbox (shipping_outbox) và bắn Kafka Event SHIPPING_BOOKING_FAILED.',
      topic: 'shipping-events',
      tech: 'Resilience4j Retry (Exponential Backoff) + Transactional Outbox Pattern',
      keyData: 'Retry: 3/3 Exhausted | Event: SHIPPING_BOOKING_FAILED (3PL Timeout)',
    },
    {
      step: 2,
      title: 'Bước 4 (Orchestrator): Ra lệnh REVERT_INVENTORY_COMMAND',
      service: 'Order Service (:8081 - Saga Orchestrator)',
      action: 'Nhận SHIPPING_BOOKING_FAILED -> Đổi trạng thái Order sang REVERTING_INVENTORY -> Ghi Outbox Command và bắn Kafka Command REVERT_INVENTORY_COMMAND sang cho Inventory Service.',
      topic: 'inventory-commands',
      tech: 'Saga Central Orchestrator + Transactional Outbox + Kafka Command Message',
      keyData: 'Order Status: REVERTING_INVENTORY | Command: REVERT_INVENTORY_COMMAND',
    },
    {
      step: 3,
      title: 'Bước 5 (Inventory): Nhả kho Atomic SQL trên PostgreSQL',
      service: 'Inventory Service (:8083 - KHÔNG DÙNG REDIS)',
      action: 'Nhận REVERT_INVENTORY_COMMAND -> Thực thi Atomic SQL UPDATE (available = available + qty) trực tiếp trên PostgreSQL -> Không dùng Redis, chống Race Condition bằng row-level lock ngầm định -> Ghi Outbox & bắn INVENTORY_RELEASED.',
      topic: 'inventory-events',
      tech: 'PostgreSQL Native Atomic UPDATE + Transactional Outbox (NO REDIS)',
      keyData: 'Stock Reverted: available += qty, reserved -= qty | Event: INVENTORY_RELEASED',
    },
    {
      step: 4,
      title: 'Bước 6 (Orchestrator): Chốt ORDER_FAILED_SHIPPING_ERROR',
      service: 'Order Service (:8081 - Saga Orchestrator)',
      action: 'Nhận INVENTORY_RELEASED -> Kiểm tra đơn đang ở REVERTING_INVENTORY -> Cập nhật trạng thái đơn thành ORDER_FAILED_SHIPPING_ERROR kèm lý do 3PL thất bại -> Hoàn tất đóng Saga Rollback an toàn!',
      topic: 'inventory-events -> Final Order State',
      tech: 'Saga State Machine Transition + Audit Trail Trail Complete',
      keyData: 'Final Status: ORDER_FAILED_SHIPPING_ERROR (Saga Rollback Closed)',
    },
  ];

  const sagaSteps = selectedScenario === 'customer' 
    ? customerSteps 
    : selectedScenario === 'fulfillment' 
      ? fulfillmentSteps 
      : shippingTimeoutSteps;

  const handleRunSimulation = () => {
    setIsSimulating(true);
    setSagaStep(1);
    const timers = [
      setTimeout(() => setSagaStep(2), 1200),
      setTimeout(() => setSagaStep(3), 2400),
      setTimeout(() => {
        setSagaStep(4);
        setIsSimulating(false);
      }, 3600),
    ];
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 font-sans p-6 md:p-10 selection:bg-teal-500 selection:text-slate-950">
      <div className="max-w-7xl mx-auto space-y-8">
        
        {/* Header Bar */}
        <header className="flex flex-col md:flex-row md:items-center justify-between pb-6 border-b border-slate-800 gap-4">
          <div>
            <div className="flex items-center gap-3">
              <span className="p-2 rounded-lg bg-teal-500/10 text-teal-400 border border-teal-500/20">
                <Cpu className="w-6 h-6" />
              </span>
              <div>
                <h1 className="text-2xl font-bold tracking-tight text-white">XCare Omnichannel Platform</h1>
                <p className="text-sm text-slate-400">Enterprise Pharmacy Retail &amp; Omnichannel E-commerce Microservices Backend</p>
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
              <CheckCircle2 className="w-3.5 h-3.5" /> Task 1: Saga Rollback Ready
            </span>
            <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/20">
              Spring Boot 3.4.2
            </span>
          </div>
        </header>

        {/* Tab Navigation */}
        <div className="flex gap-2 border-b border-slate-800 pb-2 overflow-x-auto">
          {[
            { id: 'saga', label: 'Task 1: Saga Rollback Flow (Hủy đơn & Hoàn tác)' },
            { id: 'overview', label: 'Architecture Overview' },
            { id: 'services', label: 'Microservices Topology (:8000 - :8085)' },
            { id: 'outbox', label: 'Outbox & Distributed Lock' },
            { id: 'compose', label: 'Infrastructure Specs' },
          ].map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as any)}
              className={`px-4 py-2 text-sm font-medium rounded-lg transition-all whitespace-nowrap ${
                activeTab === tab.id
                  ? 'bg-slate-800 text-teal-300 shadow-sm border border-slate-700'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-900'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab 0: Saga Rollback Scenarios */}
        {activeTab === 'saga' && (
          <div className="space-y-6">
            <div className="p-6 rounded-xl bg-slate-900 border border-slate-800 space-y-6">
              {/* Scenario Switcher */}
              <div className="flex flex-wrap gap-2 pb-4 border-b border-slate-800/80">
                <button
                  onClick={() => { setSelectedScenario('customer'); setSagaStep(0); }}
                  className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-all flex items-center gap-2 ${
                    selectedScenario === 'customer'
                      ? 'bg-teal-500 text-slate-950 shadow-md shadow-teal-500/20'
                      : 'bg-slate-950 text-slate-400 hover:text-slate-200 border border-slate-800'
                  }`}
                >
                  <RotateCcw className="w-3.5 h-3.5" />
                  Task 1: Khách hàng ấn Hủy đơn
                </button>
                <button
                  onClick={() => { setSelectedScenario('fulfillment'); setSagaStep(0); }}
                  className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-all flex items-center gap-2 ${
                    selectedScenario === 'fulfillment'
                      ? 'bg-amber-400 text-slate-950 shadow-md shadow-amber-400/20'
                      : 'bg-slate-950 text-slate-400 hover:text-slate-200 border border-slate-800'
                  }`}
                >
                  <AlertCircle className="w-3.5 h-3.5" />
                  Task 2: Sự cố kho Hủy đóng gói
                </button>
                <button
                  onClick={() => { setSelectedScenario('shipping_timeout'); setSagaStep(0); }}
                  className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-all flex items-center gap-2 ${
                    selectedScenario === 'shipping_timeout'
                      ? 'bg-sky-400 text-slate-950 shadow-md shadow-sky-400/20'
                      : 'bg-slate-950 text-slate-400 hover:text-slate-200 border border-slate-800'
                  }`}
                >
                  <Truck className="w-3.5 h-3.5" />
                  Task 3: 3PL Timeout &amp; Orchestration Rollback (Không Redis)
                </button>
              </div>

              <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
                <div>
                  <span className="text-xs uppercase tracking-wider text-teal-400 font-bold bg-teal-950/60 px-2.5 py-1 rounded border border-teal-800/60">
                    {selectedScenario === 'shipping_timeout' 
                      ? 'Orchestration Saga (Order Service as Central Orchestrator)' 
                      : `Compensating Transactions Pattern (${selectedScenario === 'customer' ? 'Task 1' : 'Task 2'})`}
                  </span>
                  <h2 className="text-xl font-bold text-white mt-2 flex items-center gap-2">
                    {selectedScenario === 'customer' ? (
                      <>
                        <RotateCcw className="w-5 h-5 text-teal-400" />
                        Kịch bản 1: Khách hàng ấn Hủy đơn thuốc &amp; Kích hoạt Saga Rollback
                      </>
                    ) : selectedScenario === 'fulfillment' ? (
                      <>
                        <AlertCircle className="w-5 h-5 text-amber-400" />
                        Kịch bản 2: Dược sĩ Hủy đóng gói tại kho (Fulfillment Failed) &amp; Rollback
                      </>
                    ) : (
                      <>
                        <Truck className="w-5 h-5 text-sky-400" />
                        Kịch bản 3: Shipping 3PL Timeout/Failure &amp; Tự động Saga Orchestration Rollback (Atomic SQL, KHÔNG REDIS)
                      </>
                    )}
                  </h2>
                  <p className="text-sm text-slate-400 mt-1 max-w-4xl">
                    {selectedScenario === 'customer' ? (
                      <>Khi đơn thuốc đang ở trạng thái <strong className="text-amber-400">Shipper đang di chuyển tới nhà thuốc</strong>, khách hàng yêu cầu hủy. Hệ thống kích hoạt hoàn tác: Hủy Ahamove 3PL &rarr; Hoàn trả tồn kho (Redisson MultiLock) &rarr; Cập nhật <strong>CANCELLED_BY_CUSTOMER</strong>.</>
                    ) : selectedScenario === 'fulfillment' ? (
                      <>Khi Shipper đã được book nhưng Dược sĩ đóng gói phát hiện <strong className="text-rose-400">thuốc bị vỡ/hỏng hoặc hết hàng tại kệ</strong> &rarr; Hủy Ahamove 3PL &rarr; Giải phóng Reserved, cách ly thuốc hỏng &rarr; Cập nhật đơn <strong>CANCELLED_OUT_OF_STOCK</strong>.</>
                    ) : (
                      <>Khi gọi API Ahamove/GHTK bị <strong className="text-rose-400">Timeout / HTTP 500</strong> &rarr; Resilience4j Retry 3 lần thất bại &rarr; Fallback lưu Outbox &amp; phát <strong>SHIPPING_BOOKING_FAILED</strong> &rarr; Order Orchestrator ra lệnh <strong>REVERT_INVENTORY_COMMAND</strong> &rarr; Inventory nhả kho bằng <strong className="text-sky-300">Atomic SQL trên PostgreSQL (KHÔNG DÙNG REDIS)</strong> &rarr; Chốt <strong>ORDER_FAILED_SHIPPING_ERROR</strong>.</>
                    )}
                  </p>
                </div>

                <button
                  onClick={handleRunSimulation}
                  disabled={isSimulating}
                  className="px-5 py-2.5 rounded-lg bg-teal-600 hover:bg-teal-500 disabled:opacity-50 text-white font-semibold text-sm flex items-center gap-2 shadow-lg shadow-teal-900/30 transition-all shrink-0"
                >
                  {isSimulating ? (
                    <>
                      <RefreshCw className="w-4 h-4 animate-spin" />
                      Đang chạy Saga Rollback...
                    </>
                  ) : (
                    <>
                      <Play className="w-4 h-4" />
                      Mô phỏng {selectedScenario === 'customer' ? 'Task 1' : selectedScenario === 'fulfillment' ? 'Task 2' : 'Task 3'} (4 Bước)
                    </>
                  )}
                </button>
              </div>

              {/* 4-Step Interactive Flow Visualizer */}
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                {sagaSteps.map((s) => {
                  const isActive = sagaStep === s.step;
                  const isDone = sagaStep > s.step;
                  return (
                    <div
                      key={s.step}
                      onClick={() => setSagaStep(s.step)}
                      className={`cursor-pointer p-4 rounded-xl border transition-all relative flex flex-col justify-between ${
                        isActive
                          ? 'bg-slate-800 border-teal-500 shadow-md shadow-teal-500/10 ring-1 ring-teal-500'
                          : isDone
                          ? 'bg-slate-900/90 border-emerald-500/40 text-slate-300'
                          : 'bg-slate-950 border-slate-800 text-slate-400 hover:border-slate-700'
                      }`}
                    >
                      <div className="space-y-2">
                        <div className="flex items-center justify-between">
                          <span className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
                            isDone ? 'bg-emerald-500 text-slate-950' : isActive ? 'bg-teal-500 text-slate-950' : 'bg-slate-800 text-slate-400'
                          }`}>
                            {isDone ? <Check className="w-3.5 h-3.5 stroke-[3]" /> : s.step}
                          </span>
                          <span className="text-[11px] font-mono px-2 py-0.5 rounded bg-slate-950 border border-slate-800 text-teal-300">
                            {s.service.split(' ')[0]}
                          </span>
                        </div>
                        <h4 className="font-semibold text-sm text-white">{s.title}</h4>
                        <p className="text-xs text-slate-400 leading-relaxed line-clamp-4">{s.action}</p>
                      </div>

                      <div className="mt-4 pt-3 border-t border-slate-800/80 space-y-1">
                        <div className="text-[10px] text-slate-400 font-mono">Topic: <span className="text-purple-300">{s.topic}</span></div>
                      </div>
                    </div>
                  );
                })}
              </div>

              {/* Selected Step Deep Dive */}
              {sagaStep > 0 && (
                <div className="p-5 rounded-xl bg-slate-950 border border-teal-500/30 space-y-3">
                  <div className="flex items-center justify-between border-b border-slate-800 pb-3">
                    <span className="text-sm font-bold text-teal-300 flex items-center gap-2">
                      <Layers className="w-4 h-4" />
                      Chi tiết kỹ thuật {sagaSteps[sagaStep - 1].title}
                    </span>
                    <span className="text-xs font-mono text-slate-400">
                      Module: <strong className="text-white">{sagaSteps[sagaStep - 1].service}</strong>
                    </span>
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-xs">
                    <div className="space-y-1">
                      <span className="text-slate-400 font-medium">Hành động kỹ thuật:</span>
                      <p className="text-slate-200 leading-relaxed">{sagaSteps[sagaStep - 1].action}</p>
                    </div>
                    <div className="space-y-1">
                      <span className="text-slate-400 font-medium">Design Pattern &amp; Cơ chế:</span>
                      <p className="text-amber-300 font-mono leading-relaxed">{sagaSteps[sagaStep - 1].tech}</p>
                    </div>
                    <div className="space-y-1">
                      <span className="text-slate-400 font-medium">Dữ liệu &amp; Khóa phân tán:</span>
                      <p className="text-emerald-300 font-mono leading-relaxed">{sagaSteps[sagaStep - 1].keyData}</p>
                    </div>
                  </div>
                </div>
              )}
            </div>

            {/* Architecture Guarantees in Task 1 */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
              <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
                <span className="font-semibold text-teal-300 flex items-center gap-2">
                  <Lock className="w-4 h-4 text-teal-400" />
                  Redisson Distributed Multi-Lock
                </span>
                <p className="text-xs text-slate-400 leading-relaxed">
                  Ở Bước 3, trước khi giải phóng kho, Inventory Service sắp xếp danh sách SKU theo thứ tự từ điển (Lexicographical Sort) để lấy Redisson MultiLock. Điều này ngăn chặn 100% rủi ro Circular Distributed Deadlock khi nhiều đơn hoàn trả/đặt cùng lúc.
                </p>
              </div>

              <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
                <span className="font-semibold text-blue-300 flex items-center gap-2">
                  <Activity className="w-4 h-4 text-blue-400" />
                  Redis Idempotent Consumer
                </span>
                <p className="text-xs text-slate-400 leading-relaxed">
                  Kafka đảm bảo At-Least-Once delivery, do đó message có thể bị gửi trùng lặp khi rebalance. Shipping Service &amp; Inventory Service áp dụng Redis Key <code className="text-blue-300 font-mono">xcare:idempotency:*:&#123;orderId&#125;</code> với lệnh <code className="text-blue-300 font-mono">setIfAbsent</code> và TTL 24h để đảm bảo chỉ xử lý đúng 1 lần.
                </p>
              </div>

              <div className="p-5 rounded-xl bg-slate-900 border border-slate-800 space-y-2">
                <span className="font-semibold text-purple-300 flex items-center gap-2">
                  <Database className="w-4 h-4 text-purple-400" />
                  Transactional Outbox Pattern
                </span>
                <p className="text-xs text-slate-400 leading-relaxed">
                  Khi khách ấn hủy đơn ở Bước 1, trạng thái <code className="text-purple-300 font-mono">CANCEL_REQUESTED</code> và bản ghi sự kiện Outbox được ghi vào PostgreSQL trong cùng 1 ACID Transaction duy nhất. Worker nền sử dụng <code className="text-purple-300 font-mono">FOR UPDATE SKIP LOCKED</code> đẩy tin sang Kafka an toàn tuyệt đối.
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Tab 1: Overview */}
        {activeTab === 'overview' && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {stack.map((item, idx) => (
                <div key={idx} className="p-4 rounded-xl bg-slate-900 border border-slate-800 flex flex-col justify-between">
                  <span className="text-xs uppercase tracking-wider text-slate-400 font-semibold">{item.label}</span>
                  <span className="text-sm font-mono text-teal-300 font-semibold mt-2">{item.val}</span>
                </div>
              ))}
            </div>

            <div className="p-6 rounded-xl bg-slate-900 border border-slate-800 space-y-4">
              <h3 className="text-lg font-semibold text-white flex items-center gap-2">
                <ShieldCheck className="w-5 h-5 text-teal-400" /> Architectural Guarantees &amp; Implementation Principles
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                <div className="p-4 rounded-lg bg-slate-950/60 border border-slate-800/80 space-y-2">
                  <span className="font-semibold text-emerald-400 flex items-center gap-2">
                    <Lock className="w-4 h-4" /> Redisson Distributed Lock
                  </span>
                  <p className="text-slate-400 text-xs leading-relaxed">
                    Orders lock requested SKU keys lexicographically across Redis 7.2. Prevents distributed circular deadlocks and eliminates inventory overselling during high-traffic flash sales.
                  </p>
                </div>
                <div className="p-4 rounded-lg bg-slate-950/60 border border-slate-800/80 space-y-2">
                  <span className="font-semibold text-blue-400 flex items-center gap-2">
                    <Activity className="w-4 h-4" /> Transactional Outbox
                  </span>
                  <p className="text-slate-400 text-xs leading-relaxed">
                    Atomically writes orders and event records to <code className="text-blue-300 font-mono">outbox_events</code> in a single DB transaction. Polled via PostgreSQL <code className="text-blue-300 font-mono">FOR UPDATE SKIP LOCKED</code>.
                  </p>
                </div>
                <div className="p-4 rounded-lg bg-slate-950/60 border border-slate-800/80 space-y-2">
                  <span className="font-semibold text-purple-400 flex items-center gap-2">
                    <Database className="w-4 h-4" /> Database per Service
                  </span>
                  <p className="text-slate-400 text-xs leading-relaxed">
                    PostgreSQL 16 provisions separate isolated databases: <code className="text-purple-300 font-mono">orders_db</code>, <code className="text-purple-300 font-mono">auth_db</code>, <code className="text-purple-300 font-mono">shipping_db</code>, managed via Liquibase 4.29.2.
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Tab 2: Microservices Topology */}
        {activeTab === 'services' && (
          <div className="space-y-4">
            <div className="overflow-x-auto rounded-xl border border-slate-800">
              <table className="w-full text-left text-sm">
                <thead className="bg-slate-900 text-slate-300 uppercase text-xs tracking-wider border-b border-slate-800">
                  <tr>
                    <th className="p-4">Service Name</th>
                    <th className="p-4">Port</th>
                    <th className="p-4">Primary Technology</th>
                    <th className="p-4">Target Database</th>
                    <th className="p-4">Core Responsibility</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/60 bg-slate-950">
                  {microservices.map((svc, i) => (
                    <tr key={i} className="hover:bg-slate-900/50 transition-colors">
                      <td className="p-4 font-semibold text-white flex items-center gap-2">
                        <Server className="w-4 h-4 text-teal-400" />
                        {svc.name}
                      </td>
                      <td className="p-4 font-mono text-teal-300">:{svc.port}</td>
                      <td className="p-4 text-slate-300 font-mono text-xs">{svc.tech}</td>
                      <td className="p-4 font-mono text-xs text-purple-300">{svc.db}</td>
                      <td className="p-4 text-slate-400 text-xs">{svc.role}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Tab 3: Outbox Flow */}
        {activeTab === 'outbox' && (
          <div className="p-6 rounded-xl bg-slate-900 border border-slate-800 space-y-6">
            <h3 className="text-lg font-semibold text-white">Order Creation &amp; Outbox Event Flow</h3>
            <div className="grid grid-cols-1 md:grid-cols-5 gap-3 text-xs">
              <div className="p-4 rounded-lg bg-slate-950 border border-slate-800 space-y-2">
                <div className="font-bold text-teal-400">1. Client Request</div>
                <p className="text-slate-400">POST /api/v1/orders with customerId, hubId, SKUs and prescription metadata.</p>
              </div>
              <div className="p-4 rounded-lg bg-slate-950 border border-slate-800 space-y-2">
                <div className="font-bold text-teal-400">2. Redisson Lock</div>
                <p className="text-slate-400">Sorts SKUs alphabetically and executes multi-lock with 5s wait / 10s lease time.</p>
              </div>
              <div className="p-4 rounded-lg bg-slate-950 border border-slate-800 space-y-2">
                <div className="font-bold text-teal-400">3. ACID Transaction</div>
                <p className="text-slate-400">Inserts into `orders`, `order_items`, and `outbox_events` (status: PENDING) simultaneously.</p>
              </div>
              <div className="p-4 rounded-lg bg-slate-950 border border-slate-800 space-y-2">
                <div className="font-bold text-teal-400">4. Outbox Poller</div>
                <p className="text-slate-400">Background worker polls pending events using SKIP LOCKED and publishes to Kafka 3.7.0.</p>
              </div>
              <div className="p-4 rounded-lg bg-slate-950 border border-slate-800 space-y-2">
                <div className="font-bold text-teal-400">5. Saga Consumers</div>
                <p className="text-slate-400">Hub Fulfillment, Shipping 3PL, and Notification services consume event with idempotency.</p>
              </div>
            </div>
          </div>
        )}

        {/* Tab 4: Infrastructure */}
        {activeTab === 'compose' && (
          <div className="p-6 rounded-xl bg-slate-900 border border-slate-800 space-y-4">
            <h3 className="text-lg font-semibold text-white flex items-center gap-2">
              <Terminal className="w-5 h-5 text-teal-400" /> Docker Infrastructure Manifest
            </h3>
            <p className="text-sm text-slate-400">
              Run <code className="text-teal-300 font-mono bg-slate-950 px-2 py-0.5 rounded border border-slate-800">docker compose up -d</code> in the root directory to launch all underlying platforms:
            </p>
            <ul className="list-disc list-inside text-sm text-slate-300 space-y-1 font-mono">
              <li>PostgreSQL 16: Port 5432 (Auto-initializes keycloak_db, auth_db, orders_db, shipping_db, fulfillment_db, inventory_db, tracking_db)</li>
              <li>Apache Kafka 3.7.0: Ports 9092 (internal), 29092 (host) (KRaft Mode, Cluster ID initialized)</li>
              <li>Kafka UI: Port 8090 (Dashboard at http://localhost:8090)</li>
              <li>Redis 7.2: Port 6379 (Password protected for Redisson Distributed Locks)</li>
              <li>Keycloak 24.0.2: Port 8088 (OIDC IAM Realm &amp; Token Exchange enabled)</li>
            </ul>
          </div>
        )}

      </div>
    </div>
  );
}
