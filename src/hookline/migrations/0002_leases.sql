-- ADR-0002: a claim records who holds the row and until when. An expired lease on a
-- 'delivering' row means the worker died; the claim query treats it like 'pending'.

ALTER TABLE deliveries
    ADD COLUMN locked_by    text,
    ADD COLUMN locked_until timestamptz;

DROP INDEX deliveries_pending_idx;
-- what the claim query scans: pending rows, and delivering rows whose lease has expired
CREATE INDEX deliveries_claim_idx ON deliveries (status, locked_until, id)
    WHERE status IN ('pending', 'delivering');
