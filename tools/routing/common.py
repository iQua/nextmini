"""
Database access layer adapted for new nextmini architecture.
"""
import json
from collections import defaultdict
import psycopg2


class Database:
    def __init__(self, db_creds: dict):
        self.connection = psycopg2.connect(**db_creds)
        self.connection.autocommit = True
        self.print_buffer = []

    def get_cursor(self):
        return self.connection.cursor()

    def get_all_routes(self):
        """
        Fetch all routes from database.
        Returns: [(src_node_id, dst_node_id, route_id, edges), ...]
        Note: edges is JSONB type that needs to be parsed.
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_node_id, dst_node_id, route_id, edges
            FROM routes
            ORDER BY src_node_id, dst_node_id, route_id
        """)
        results = []
        for row in cursor.fetchall():
            src, dst, route_id, edges_json = row
            edges = json.loads(edges_json) if isinstance(edges_json, str) else edges_json
            results.append((src, dst, route_id, edges))
        cursor.close()
        return results

    def get_all_nodes(self):
        """Fetch all node IDs from database."""
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT id FROM nodes ORDER BY id
        """)
        result = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return result

    def get_active_flows(self):
        """
        Fetch all active flows with their statistics.
        Returns: [(flow_id_bytes, src_node_id, dst_node_id, route_id, bps), ...]
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            WITH flow_metrics AS (
                SELECT 
                    m.flow_id,
                    m.bytes,
                    m.time_read,
                    LAG(m.bytes) OVER (PARTITION BY m.flow_id ORDER BY m.time_read) as prev_bytes,
                    LAG(m.time_read) OVER (PARTITION BY m.flow_id ORDER BY m.time_read) as prev_time,
                    ROW_NUMBER() OVER (PARTITION BY m.flow_id ORDER BY m.time_read DESC) as rn
                FROM metrics m
                WHERE m.time_read > NOW() - INTERVAL '60 seconds'
            )
            SELECT 
                af.flow_id,
                af.src_node_id,
                af.dst_node_id,
                af.route_id,
                COALESCE(
                    CASE 
                        WHEN fm.prev_bytes IS NOT NULL AND fm.prev_time IS NOT NULL 
                            AND EXTRACT(EPOCH FROM (fm.time_read - fm.prev_time)) > 0
                            AND fm.bytes >= fm.prev_bytes
                        THEN (fm.bytes - fm.prev_bytes) * 8.0 / 
                             EXTRACT(EPOCH FROM (fm.time_read - fm.prev_time))
                        ELSE 0
                    END,
                    0
                ) as bps
            FROM app_flows af
            LEFT JOIN flow_metrics fm ON af.flow_id = fm.flow_id AND fm.rn = 1
            WHERE af.is_finished = FALSE
              AND af.route_id IS NOT NULL
        """)
        result = cursor.fetchall()
        cursor.close()
        return result

    def get_route_utilization(self):
        """
        Get current utilization (total traffic) of each route.
        Uses simple sum over time window (same as dashboard).
        Returns: {(src_node_id, dst_node_id, route_id): bps, ...}
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT af.src_node_id,
                   af.dst_node_id,
                   af.route_id,
                   SUM(m.bytes * 8.0 / 20.0) as total_rate_bps
            FROM app_flows af
            LEFT JOIN metrics m ON af.flow_id = m.flow_id
            WHERE af.is_finished = FALSE
              AND af.route_id IS NOT NULL
              AND m.time_read >= NOW() - INTERVAL '20 seconds'
            GROUP BY af.src_node_id, af.dst_node_id, af.route_id
        """)
        result = {}
        for row in cursor.fetchall():
            result[(row[0], row[1], row[2])] = float(row[3]) if row[3] else 0.0
        cursor.close()
        return result

    def get_link_utilization(self):
        """
        Get utilization of each physical link.
        Uses simple sum over time window (same as dashboard).
        Returns: {(local_node_id, remote_node_id): bps, ...}
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT local_node_id, remote_node_id,
                   SUM(bytes * 8.0 / 20.0) AS total_rate_bps
            FROM metrics
            WHERE time_read >= NOW() - INTERVAL '20 seconds'
            GROUP BY local_node_id, remote_node_id
            HAVING COUNT(*) > 0
            ORDER BY total_rate_bps DESC
        """)
        result = {}
        for row in cursor.fetchall():
            result[(row[0], row[1])] = float(row[2]) if row[2] else 0.0
        cursor.close()
        return result

    def get_flow_distribution(self):
        """
        Get distribution of flows across routes.
        Returns: {(src, dst): {route_id: count, ...}, ...}
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT src_node_id, dst_node_id, route_id, COUNT(*) as flow_count
            FROM app_flows
            WHERE is_finished = FALSE AND route_id IS NOT NULL
            GROUP BY src_node_id, dst_node_id, route_id
        """)
        result = defaultdict(dict)
        for row in cursor.fetchall():
            result[(row[0], row[1])][row[2]] = row[3]
        cursor.close()
        return dict(result)

    def clear_routes(self, src_node_id, dst_node_id):
        """
        Delete all routes between specified src-dst pair.
        Warning: This will cause existing flows to re-select routes!
        """
        cursor = self.connection.cursor()
        cursor.execute("""
            DELETE FROM routes
            WHERE src_node_id = %s AND dst_node_id = %s
        """, (src_node_id, dst_node_id))
        deleted = cursor.rowcount
        cursor.close()
        return deleted

    def install_route(self, src_node_id, dst_node_id, edges):
        """
        Install a new route.
        edges: List of edges, e.g., [[1, 2], [2, 3]]
        Returns: route_id
        """
        cursor = self.connection.cursor()
        edges_json = json.dumps(edges)
        cursor.execute("""
            INSERT INTO routes (src_node_id, dst_node_id, edges)
            VALUES (%s, %s, %s)
            RETURNING route_id
        """, (src_node_id, dst_node_id, edges_json))
        route_id = cursor.fetchone()[0]
        cursor.close()
        return route_id

    def update_routes(self, src_node_id, dst_node_id, paths):
        """
        Atomically update all routes between src-dst pair.
        Keeps route_ids stable to maintain Jump Hash consistency.
        paths: List of paths, each path is a list of nodes, e.g., [[1,2,3], [1,4,3]]
        """
        cursor = self.connection.cursor()
        
        cursor.execute("BEGIN")
        
        try:
            # Get existing route IDs for this src-dst pair
            cursor.execute("""
                SELECT route_id FROM routes
                WHERE src_node_id = %s AND dst_node_id = %s
                ORDER BY route_id
            """, (src_node_id, dst_node_id))
            existing_ids = [row[0] for row in cursor.fetchall()]
            
            route_ids = []
            
            # Update existing routes or insert new ones
            for i, path in enumerate(paths):
                edges = [[path[i], path[i+1]] for i in range(len(path)-1)]
                edges_json = json.dumps(edges)
                
                if i < len(existing_ids):
                    # Update existing route
                    route_id = existing_ids[i]
                    cursor.execute("""
                        UPDATE routes
                        SET edges = %s
                        WHERE route_id = %s
                    """, (edges_json, route_id))
                    route_ids.append(route_id)
                else:
                    # Insert new route
                    cursor.execute("""
                        INSERT INTO routes (src_node_id, dst_node_id, edges)
                        VALUES (%s, %s, %s)
                        RETURNING route_id
                    """, (src_node_id, dst_node_id, edges_json))
                    route_ids.append(cursor.fetchone()[0])
            
            # Delete excess routes if we have fewer paths than before
            if len(paths) < len(existing_ids):
                excess_ids = existing_ids[len(paths):]
                cursor.execute("""
                    DELETE FROM routes
                    WHERE route_id = ANY(%s)
                """, (excess_ids,))
            
            cursor.execute("COMMIT")
            cursor.close()
            return route_ids
            
        except Exception as e:
            cursor.execute("ROLLBACK")
            cursor.close()
            raise e

    def edges_to_path(self, edges):
        """
        Convert edge list to path (node list).
        edges: [[1, 2], [2, 3]] -> path: [1, 2, 3]
        """
        if not edges:
            return []
        path = [edges[0][0]]
        for edge in edges:
            path.append(edge[1])
        return path

    def path_to_edges(self, path):
        """
        Convert path to edge list.
        path: [1, 2, 3] -> edges: [[1, 2], [2, 3]]
        """
        return [[path[i], path[i+1]] for i in range(len(path)-1)]

