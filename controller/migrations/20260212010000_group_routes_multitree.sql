-- Upgrade multicast group route persistence to support multiple trees per group.
-- Backward compatibility is preserved by materializing legacy rows as tree_id = 0.

ALTER TABLE group_routes
    ADD COLUMN IF NOT EXISTS tree_id INTEGER;

ALTER TABLE group_routes
    ADD COLUMN IF NOT EXISTS weight DOUBLE PRECISION;

UPDATE group_routes
SET tree_id = 0
WHERE tree_id IS NULL;

ALTER TABLE group_routes
    ALTER COLUMN tree_id SET DEFAULT 0;

ALTER TABLE group_routes
    ALTER COLUMN tree_id SET NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'group_routes_pkey'
          AND conrelid = 'group_routes'::regclass
    ) THEN
        ALTER TABLE group_routes DROP CONSTRAINT group_routes_pkey;
    END IF;
END;
$$;

ALTER TABLE group_routes
    ADD CONSTRAINT group_routes_pkey PRIMARY KEY (group_id, tree_id);

ALTER TABLE group_routes
    ADD CONSTRAINT group_routes_tree_id_nonnegative CHECK (tree_id >= 0);

CREATE INDEX IF NOT EXISTS idx_group_routes_group_id
    ON group_routes (group_id);

CREATE INDEX IF NOT EXISTS idx_group_routes_src_node_id
    ON group_routes (src_node_id);
