import React, { useState } from 'react';
import { 
  Server, Database, Network, ShieldCheck, Cpu, 
  Layers, CheckCircle2, Lock, ArrowRight, Activity, Terminal
} from 'lucide-react';

export default function App() {
  const [activeTab, setActiveTab] = useState<'overview' | 'services' | 'outbox' | 'compose'>('overview');

  const microservices = [
    { name: 'API Gateway', port: 8000, tech: 'Spring Cloud Gateway', db: '-', role: 'Routing, Rate Limiting & SSL Termination' },
    { name: 'Auth Service', port: 8080, tech: 'Keycloak 24.0.2 + Spring Security', db: 'auth_db', role: 'Identity Provider, Dynamic RBAC, Customer & Staff Accounts' },
    { name: 'Order Service', port: 8081, tech: 'Java 21, Spring Boot 3.4.2, Redisson', db: 'orders_db', role: 'Dynamic Pricing, Redisson Distributed Lock, Transactional Outbox' },
    { name: 'Shipping & 3PL Service', port: 8082, tech: 'Spring Boot 3.4.2 + 3PL Adapters', db: 'shipping_db', role: 'Multi-carrier routing: GHTK, GHN, ViettelPost, Ahamove, Grab' },
    { name: 'Hub Fulfillment Service', port: 8083, tech: 'Spring Boot 3.4.2 + Kafka Consumer', db: 'fulfillment_db', role: 'Pharmacy Store/Warehouse Batch Picking & Handover barcode' },
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
                <p className="text-sm text-slate-400">Enterprise Pharmacy Retail &amp; Omnichannel E-commerce Backend Architecture</p>
              </div>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
              <CheckCircle2 className="w-3.5 h-3.5" /> Backend Ready
            </span>
            <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-xs font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/20">
              Spring Boot 3.4.2
            </span>
          </div>
        </header>

        {/* Tab Navigation */}
        <div className="flex gap-2 border-b border-slate-800 pb-2">
          {[
            { id: 'overview', label: 'Architecture Overview' },
            { id: 'services', label: 'Microservices Topology (:8000 - :8085)' },
            { id: 'outbox', label: 'Outbox & Distributed Lock Flow' },
            { id: 'compose', label: 'Infrastructure Specs' },
          ].map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as any)}
              className={`px-4 py-2 text-sm font-medium rounded-lg transition-all ${
                activeTab === tab.id
                  ? 'bg-slate-800 text-teal-300 shadow-sm border border-slate-700'
                  : 'text-slate-400 hover:text-slate-200 hover:bg-slate-900'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

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
              <li>PostgreSQL 16: Port 5432 (Auto-initializes keycloak_db, auth_db, orders_db, shipping_db, fulfillment_db, tracking_db)</li>
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
