"""
Nextmini Database Adapter for LP Multicast.

Provides functions to inject LP-computed multicast trees
directly into Nextmini's database.
"""
import typing as t
import json
from dataclasses import dataclass

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("Warning: psycopg2 not installed. Install with: pip install psycopg2-binary")


@dataclass
class Group:
    """Represents a Nextmini multicast group."""
    id: int
    label: str
    src_node_id: int
    group_ip: str


class NextminiAdapter:
    """
    Adapter for injecting LP multicast solutions into Nextmini.
    
    Usage:
        adapter = NextminiAdapter(db_config)
        group = adapter.create_group("my-session", src_node_id=1)
        adapter.inject_tree(group.id, src_node_id=1, edges=[(1,2), (2,3), (2,4)])
        adapter.notify_update(group.id)
    """
    
    def __init__(self, db_config: dict):
        """
        Initialize adapter with database configuration.
        
        Args:
            db_config: Dict with keys: host, port, user, password, database
        """
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 is required for database access")
        
        self.conn = psycopg2.connect(
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 5432),
            user=db_config.get("user", "pgusr"),
            password=db_config.get("password", "pgpwrd"),
            database=db_config.get("database", "nextmini"),
        )
        self._next_group_ip_offset = 1
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def _allocate_group_ip(self, base: str = "239.255.0.0") -> str:
        """Allocate a unique multicast group IP."""
        # Simple allocation: increment offset
        parts = base.split(".")
        base_int = (int(parts[0]) << 24) | (int(parts[1]) << 16) | (int(parts[2]) << 8) | int(parts[3])
        new_int = base_int + self._next_group_ip_offset
        self._next_group_ip_offset += 1
        
        return f"{(new_int >> 24) & 0xFF}.{(new_int >> 16) & 0xFF}.{(new_int >> 8) & 0xFF}.{new_int & 0xFF}"
    
    def create_group(self, label: str, src_node_id: int) -> Group:
        """
        Create a new multicast group (LP-managed).
        
        Args:
            label: Human-readable label for the group
            src_node_id: Source node ID for this multicast session
            
        Returns:
            Group object with id, label, src_node_id, group_ip
        """
        group_ip = self._allocate_group_ip()
        
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO groups (label, src_node_id, group_ip)
                VALUES (%s, %s, %s)
                RETURNING id, label, src_node_id, group_ip
                """,
                (label, src_node_id, group_ip)
            )
            row = cur.fetchone()
            self.conn.commit()
            
        return Group(id=row[0], label=row[1], src_node_id=row[2], group_ip=row[3])
    
    def inject_tree(
        self, 
        group_id: int, 
        src_node_id: int, 
        edges: t.List[t.Tuple[int, int]]
    ):
        """
        Inject LP-computed tree edges into group_routes table.
        
        Args:
            group_id: Multicast group ID
            src_node_id: Source node ID
            edges: List of (from_node, to_node) edges forming the tree
        """
        # Convert edges to JSON format
        edges_json = json.dumps([[e[0], e[1]] for e in edges])
        
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO group_routes (group_id, src_node_id, edges)
                VALUES (%s, %s, %s)
                ON CONFLICT (group_id)
                DO UPDATE SET 
                    src_node_id = EXCLUDED.src_node_id, 
                    edges = EXCLUDED.edges,
                    updated_at = EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
                """,
                (group_id, src_node_id, edges_json)
            )
            self.conn.commit()
    
    def inject_members(
        self,
        group_id: int,
        member_node_ids: t.List[int]
    ):
        """
        Inject member node IDs into group_members table.
        
        Args:
            group_id: Multicast group ID
            member_node_ids: List of destination node IDs
        """
        with self.conn.cursor() as cur:
            for node_id in member_node_ids:
                cur.execute(
                    """
                    INSERT INTO group_members (group_id, node_id)
                    VALUES (%s, %s)
                    ON CONFLICT (group_id, node_id) DO NOTHING
                    """,
                    (group_id, node_id)
                )
            self.conn.commit()
    
    def notify_update(self, group_id: int):
        """
        Trigger pg_notify to push routes to dataplane.
        
        This tells the Nextmini controller to read the edges
        from group_routes and push them to all relevant nodes.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT pg_notify('sync_group_routes', %s)",
                (json.dumps({"group_id": str(group_id)}),)
            )
            self.conn.commit()
    
    def get_all_groups(self) -> t.List[Group]:
        """Get all existing multicast groups."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT id, label, src_node_id, group_ip FROM groups ORDER BY id")
            rows = cur.fetchall()
        
        return [Group(id=r[0], label=r[1], src_node_id=r[2], group_ip=r[3]) for r in rows]
    
    def delete_group(self, group_id: int):
        """Delete a multicast group and its routes."""
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM group_routes WHERE group_id = %s", (group_id,))
            cur.execute("DELETE FROM groups WHERE id = %s", (group_id,))
            self.conn.commit()
    
    def clear_all_groups(self) -> int:
        """Delete all multicast groups."""
        with self.conn.cursor() as cur:
            # Get all group IDs
            cur.execute("SELECT id FROM groups")
            all_groups = [row[0] for row in cur.fetchall()]
            
            # Delete their routes, members, and groups
            for gid in all_groups:
                cur.execute("DELETE FROM group_routes WHERE group_id = %s", (gid,))
                cur.execute("DELETE FROM group_members WHERE group_id = %s", (gid,))
                cur.execute("DELETE FROM groups WHERE id = %s", (gid,))
            
            self.conn.commit()
            return len(all_groups)
