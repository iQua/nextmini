-- Ensure group membership notifications include the touched node id so
-- leave events can clear stale member-local multicast routes.
CREATE OR REPLACE FUNCTION notify_group_membership_change()
RETURNS TRIGGER AS $$
DECLARE gid INTEGER;
DECLARE nid INTEGER;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        gid := NEW.group_id;
        nid := NEW.node_id;
    ELSE
        gid := OLD.group_id;
        nid := OLD.node_id;
    END IF;

    PERFORM pg_notify(
        'sync_group_routes',
        '{"group_id":"' || gid || '","node_id":"' || nid || '"}'
    );
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
