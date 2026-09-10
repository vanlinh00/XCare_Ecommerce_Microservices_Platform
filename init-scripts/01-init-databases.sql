-- ====================================================================
-- XCare Omnichannel Platform - Database Initialization Script
-- Engine: PostgreSQL 16 (Database per Service pattern)
-- ====================================================================

-- 1. Keycloak IAM Database
SELECT 'CREATE DATABASE keycloak_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'keycloak_db')\gexec

-- 2. Auth Service Database (Local User Profile, Dynamic RBAC, Permissions)
SELECT 'CREATE DATABASE auth_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'auth_db')\gexec

-- 3. Order Service Database (Orders, Order Items, Dynamic Pricing, Outbox)
SELECT 'CREATE DATABASE orders_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'orders_db')\gexec

-- 4. Shipping & 3PL Integration Database (GHTK, GHN, ViettelPost, Ahamove, Grab)
SELECT 'CREATE DATABASE shipping_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'shipping_db')\gexec

-- 5. Hub Fulfillment Database (Warehouse & Pharmacy Store Packing, Handover)
SELECT 'CREATE DATABASE fulfillment_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'fulfillment_db')\gexec

-- 6. Tracking & Webhook Database (Real-time tracking, 3PL Webhook idempotency)
SELECT 'CREATE DATABASE tracking_db'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'tracking_db')\gexec

-- Grant privileges to default application user
GRANT ALL PRIVILEGES ON DATABASE keycloak_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE auth_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE orders_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE shipping_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE fulfillment_db TO postgres;
GRANT ALL PRIVILEGES ON DATABASE tracking_db TO postgres;
