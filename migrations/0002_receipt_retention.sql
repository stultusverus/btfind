ALTER TABLE persistence_receipts
    ADD COLUMN acknowledged_at BIGINT NULL,
    ADD INDEX idx_receipt_acknowledged(acknowledged_at);
