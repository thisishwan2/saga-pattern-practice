-- Service 1: Account Service Database

CREATE TABLE accounts (
    account_id VARCHAR(36) PRIMARY KEY,
    account_number VARCHAR(20) NOT NULL UNIQUE,
    balance DECIMAL(15, 2) NOT NULL DEFAULT 0.00,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE account_transactions (
    transaction_id VARCHAR(36) PRIMARY KEY,
    account_id VARCHAR(36) NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL,
    saga_id VARCHAR(36),
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE saga_state (
    saga_id VARCHAR(36) PRIMARY KEY,
    pattern_type VARCHAR(20) NOT NULL,
    from_account_id VARCHAR(36) NOT NULL,
    to_account_id VARCHAR(36) NOT NULL,
    amount DECIMAL(15, 2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO accounts (account_id, account_number, balance, status) VALUES
    ('acc-001', '1000-0001', 1000000.00, 'ACTIVE'),
    ('acc-002', '1000-0002', 500000.00, 'ACTIVE'),
    ('acc-003', '1000-0003', 2000000.00, 'ACTIVE');

