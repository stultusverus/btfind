ALTER TABLE torrents
    DROP INDEX idx_first,
    DROP INDEX idx_complete_first,
    DROP INDEX idx_name;
