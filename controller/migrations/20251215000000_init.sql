-- Initial controller database schema.

-- private_network_name: Used to identify which private network (cluster) the node belongs to.
-- private_network_addr: Address of the node in the private network.
-- public_network_addr: Address of the node in the public network, when connecting to other private networks
-- over the public internet.
CREATE TABLE IF NOT EXISTS nodes (
    id SERIAL PRIMARY KEY,
    private_network_name TEXT,
    private_network_addr TEXT NOT NULL,
    public_network_addr TEXT NOT NULL
);

-- id: Unique identifier for the flow, automatically assigned by controller.
-- src_node_id: Source node ID for the flow.
-- dst_node_id: Destination node ID for the flow.
-- flow_len_type: Type of flow length specification ('bytes' or 'duration').
-- flow_len_bytes: Flow length in bytes (used when flow_len_type is 'bytes').
-- flow_len_duration: Flow length in seconds (used when flow_len_type is 'duration').
-- flow_rate: Optional flow rate in bytes per second.
-- flow_weight: Optional flow weight for scheduling.
-- start_time: When a flow starts.
-- finish_time: When a flow finishes.
-- is_finished: Whether this flow has completed.
-- is_probe: Whether this flow is a probe-only measurement flow.
CREATE TABLE IF NOT EXISTS flows (
    id SERIAL PRIMARY KEY,
    src_node_id INTEGER NOT NULL,
    dst_node_id INTEGER NOT NULL,
    flow_len_type TEXT NOT NULL CHECK (flow_len_type IN ('bytes', 'duration')),
    flow_len_bytes BIGINT,
    flow_len_duration DOUBLE PRECISION,
    flow_rate INTEGER,
    flow_weight INTEGER,
    start_time BIGINT,
    finish_time BIGINT,
    is_finished BOOLEAN NOT NULL DEFAULT FALSE,
    is_probe BOOLEAN NOT NULL DEFAULT FALSE
);

-- route_id: Unique identifier for the route, automatically assigned by controller.
-- src_node_id: Source node ID for the route.
-- dst_node_id: Destination node ID for the route.
-- edges: All edges in the route, as an array of node IDs. e.g. [[1, 2], [2, 3]]
CREATE TABLE IF NOT EXISTS routes (
    route_id SERIAL PRIMARY KEY,
    src_node_id INTEGER NOT NULL,
    dst_node_id INTEGER NOT NULL,
    edges JSONB NOT NULL
);

-- flow_routes: Relationship table documenting route assignments for flows.
-- This supports "route pinning": forcing a flow to use a specific controller-provided route_id.
CREATE TABLE IF NOT EXISTS flow_routes (
    flow_id INTEGER NOT NULL,
    route_id INTEGER NOT NULL,
    PRIMARY KEY (flow_id, route_id),
    FOREIGN KEY (flow_id) REFERENCES flows(id) ON DELETE CASCADE,
    FOREIGN KEY (route_id) REFERENCES routes(route_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_flow_routes_flow_id ON flow_routes(flow_id);

CREATE TABLE IF NOT EXISTS metrics (
    id SERIAL PRIMARY KEY,
    flow_id BYTEA NOT NULL,
    local_node_id INTEGER NOT NULL,
    remote_node_id INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    time_read TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE IF NOT EXISTS app_flows (
    id SERIAL PRIMARY KEY,
    flow_id BYTEA NOT NULL,
    start_time BIGINT NOT NULL,
    src_node_id INTEGER,
    dst_node_id INTEGER,
    is_finished BOOLEAN NOT NULL DEFAULT FALSE,
    finish_time BIGINT,
    route_id INTEGER,
    UNIQUE (flow_id, start_time)
);

CREATE TABLE IF NOT EXISTS groups (
    id SERIAL PRIMARY KEY,
    label TEXT UNIQUE NOT NULL,
    src_node_id INTEGER NOT NULL,
    group_ip TEXT UNIQUE NOT NULL,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT * 1000)
);

CREATE TABLE IF NOT EXISTS group_members (
    group_id INTEGER NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
    node_id INTEGER NOT NULL,
    joined_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT * 1000),
    PRIMARY KEY (group_id, node_id)
);

CREATE TABLE IF NOT EXISTS group_routes (
    group_id INTEGER NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
    src_node_id INTEGER NOT NULL,
    edges JSONB NOT NULL,
    updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT * 1000),
    PRIMARY KEY (group_id)
);

-- Group membership changes: recompute multicast routes.
CREATE OR REPLACE FUNCTION notify_group_membership_change()
RETURNS TRIGGER AS $$
DECLARE gid INTEGER;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        gid := NEW.group_id;
    ELSE
        gid := OLD.group_id;
    END IF;
    PERFORM pg_notify('sync_group_routes', '{"group_id":"' || gid || '"}');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS group_membership_change_trigger ON group_members;
CREATE TRIGGER group_membership_change_trigger
AFTER INSERT OR DELETE ON group_members
FOR EACH ROW
EXECUTE FUNCTION notify_group_membership_change();

-- Routes changed: prompt dataplane route sync.
CREATE OR REPLACE FUNCTION notify_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('auto_sync_routes', '{"op":"' || TG_OP || '","route_id":"' || NEW.route_id || '"}');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS new_flow_trigger ON routes;
CREATE TRIGGER new_flow_trigger
AFTER INSERT OR UPDATE ON routes
FOR EACH ROW
EXECUTE FUNCTION notify_trigger_function();

-- Flows inserted: prompt dataplane flow install.
CREATE OR REPLACE FUNCTION notify_flow_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('auto_sync_flows', '{"newly_inserted_id":"'|| NEW.id || '","src_node_id":"' || NEW.src_node_id || '","dst_node_id":"' || NEW.dst_node_id || '"}');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS flow_notification_trigger ON flows;
CREATE TRIGGER flow_notification_trigger
AFTER INSERT ON flows
FOR EACH ROW
EXECUTE FUNCTION notify_flow_trigger_function();
