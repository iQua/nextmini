#!/usr/bin/env python3
import psycopg2  # pip install psycopg2-binary
import time
import sys
from datetime import datetime
from collections import defaultdict
from tabulate import tabulate

# Database connection configuration
DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "nextmini",
    "user": "pgusr",
    "password": "pgpwrd"
}

# Flow type identification
FLOW_TYPES = {
    (3, 4): "Inference Request",
    (1, 4): "Weight Update (T1->I)",
    (2, 4): "Weight Update (T2->I)",
    (1, 2): "NCCL Sync (T1<->T2)",
    (2, 1): "NCCL Sync (T2<->T1)",
}

def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"ERROR: Cannot connect to database: {e}")
        sys.exit(1)

def identify_flow_type(src_node_id, dst_node_id):
    """Identify flow type based on source and destination"""
    key = (src_node_id, dst_node_id)
    return FLOW_TYPES.get(key, f"Unknown ({src_node_id}->{dst_node_id})")

def get_active_flows(conn):
    """Get all active flows"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            af.flow_id,
            af.src_node_id,
            af.dst_node_id,
            af.is_finished,
            afr.route_id,
            r.edges
        FROM app_flows af
        LEFT JOIN app_flow_routes afr ON af.flow_id = afr.flow_id
        LEFT JOIN routes r ON afr.route_id = r.route_id
        WHERE af.is_finished = FALSE
        ORDER BY af.id DESC;
    """)
    return cursor.fetchall()

def get_flow_statistics(conn):
    """Get flow statistics"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            src_node_id,
            dst_node_id,
            COUNT(*) as total_flows,
            SUM(CASE WHEN is_finished THEN 1 ELSE 0 END) as finished_flows,
            SUM(CASE WHEN is_finished THEN 0 ELSE 1 END) as active_flows
        FROM app_flows
        GROUP BY src_node_id, dst_node_id
        ORDER BY total_flows DESC;
    """)
    return cursor.fetchall()

def get_route_usage(conn):
    """Get route usage statistics"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT
            r.route_id,
            r.src_node_id,
            r.dst_node_id,
            r.edges,
            COUNT(afr.flow_id) as flow_count
        FROM routes r
        LEFT JOIN app_flow_routes afr ON r.route_id = afr.route_id
        GROUP BY r.route_id, r.src_node_id, r.dst_node_id, r.edges
        HAVING COUNT(afr.flow_id) > 0
        ORDER BY flow_count DESC;
    """)
    return cursor.fetchall()

def format_flow_id(flow_id_bytes):
    """Format flow_id as hex string"""
    if flow_id_bytes:
        hex_str = flow_id_bytes.hex()
        return f"{hex_str[:8]}...{hex_str[-8:]}"
    return "N/A"

def format_route(edges):
    """Format route path"""
    if edges:
        path = " → ".join([str(edge[0]) for edge in edges] + [str(edges[-1][1])])
        return path
    return "N/A"

def display_active_flows(flows):
    """Display active flows"""
    print("\n" + "="*80)
    print("Active Flows")
    print("="*80)

    if not flows:
        print("  No active flows")
        return

    table_data = []
    for flow in flows:
        flow_id, src, dst, finished, route_id, edges = flow
        flow_type = identify_flow_type(src, dst)
        route_path = format_route(edges) if edges else "Unassigned"

        table_data.append([
            format_flow_id(flow_id),
            f"{src} -> {dst}",
            flow_type,
            route_id if route_id else "N/A",
            route_path
        ])

    headers = ["Flow ID", "Direction", "Type", "Route ID", "Path"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def display_statistics(stats):
    """Display statistics"""
    print("\n" + "="*80)
    print("Flow Statistics")
    print("="*80)

    if not stats:
        print("  No statistics available")
        return

    table_data = []
    for stat in stats:
        src, dst, total, finished, active = stat
        flow_type = identify_flow_type(src, dst)
        table_data.append([
            flow_type,
            f"{src} -> {dst}",
            total,
            finished,
            active
        ])

    headers = ["Flow Type", "Direction", "Total", "Finished", "Active"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def display_route_usage(routes):
    """Display route usage"""
    print("\n" + "="*80)
    print("Route Usage")
    print("="*80)

    if not routes:
        print("  No route usage data")
        return

    table_data = []
    for route in routes:
        route_id, src, dst, edges, count = route
        path = format_route(edges)
        table_data.append([
            route_id,
            f"{src} -> {dst}",
            path,
            count
        ])

    headers = ["Route ID", "Direction", "Path", "Flow Count"]
    print(tabulate(table_data, headers=headers, tablefmt="grid"))

def monitor_continuous():
    """Continuous monitoring mode"""
    print("Starting Prime-RL Flow Monitor")
    print("Press Ctrl+C to stop\n")

    conn = get_db_connection()

    try:
        while True:
            print(f"\nUpdate time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            active_flows = get_active_flows(conn)
            display_active_flows(active_flows)

            stats = get_flow_statistics(conn)
            display_statistics(stats)

            routes = get_route_usage(conn)
            display_route_usage(routes)

            print("\n" + "="*80)
            print(f"Next update in 10 seconds...")

            time.sleep(10)

    except KeyboardInterrupt:
        print("\n\nMonitoring stopped")
    finally:
        conn.close()

def monitor_once():
    """Single monitoring snapshot"""
    conn = get_db_connection()

    try:
        print(f"Query time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        active_flows = get_active_flows(conn)
        display_active_flows(active_flows)

        stats = get_flow_statistics(conn)
        display_statistics(stats)

        routes = get_route_usage(conn)
        display_route_usage(routes)

    finally:
        conn.close()

def main():
    """Main function"""
    if len(sys.argv) > 1 and sys.argv[1] == "--once":
        monitor_once()
    else:
        monitor_continuous()

if __name__ == "__main__":
    main()
