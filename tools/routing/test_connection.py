#!/usr/bin/env python3.13
"""
Test database connection and data retrieval.

Usage:
    uv run --python 3.13 test_connection.py
    
Requires:
    - Python 3.13
    - uv package manager
    - psycopg2-binary (install with: uv pip install psycopg2-binary)
"""

from common import Database
import sys


def test_connection():
    """Test database connection and basic queries."""
    print("=" * 60)
    print("Testing Nextmini Routing Algorithm - Database Connection")
    print("=" * 60)
    
    creds = {
        "user": "pgusr",
        "password": "pgpwrd",
        "host": "127.0.0.1",
        "port": "5432",
        "database": "nextmini",
    }
    
    try:
        print("\n1. Connecting to database...")
        db = Database(creds)
        print("   ✓ Connected successfully!")
        
        print("\n2. Testing get_all_nodes()...")
        nodes = db.get_all_nodes()
        print(f"   ✓ Found {len(nodes)} nodes: {nodes}")
        
        print("\n3. Testing get_all_routes()...")
        routes = db.get_all_routes()
        print(f"   ✓ Found {len(routes)} routes")
        if routes:
            print("   Sample routes:")
            for src, dst, route_id, edges in routes[:3]:
                path = db.edges_to_path(edges)
                print(f"     Route {route_id}: {src} → {dst} via {path}")
        
        print("\n4. Testing get_active_flows()...")
        flows = db.get_active_flows()
        print(f"   ✓ Found {len(flows)} active flows")
        if flows:
            print("   Sample flows:")
            for flow_id, src, dst, route_id, bps in flows[:3]:
                print(f"     Flow {flow_id.hex()[:8]}...: {src}→{dst} via route {route_id}, {bps/1e6:.2f} Mbps")
        
        print("\n5. Testing get_route_utilization()...")
        route_util = db.get_route_utilization()
        print(f"   ✓ Found utilization for {len(route_util)} routes")
        if route_util:
            total_bps = sum(route_util.values())
            print(f"   Total traffic: {total_bps/1e9:.4f} Gbps")
            print("   Top 5 busiest routes:")
            sorted_routes = sorted(route_util.items(), key=lambda x: x[1], reverse=True)
            for (src, dst, route_id), bps in sorted_routes[:5]:
                print(f"     Route {route_id} ({src}→{dst}): {bps/1e6:.2f} Mbps")
        
        print("\n6. Testing get_link_utilization()...")
        link_util = db.get_link_utilization()
        print(f"   ✓ Found utilization for {len(link_util)} links")
        if link_util:
            total_bps = sum(link_util.values())
            print(f"   Total link traffic: {total_bps/1e9:.4f} Gbps")
            print("   Top 5 busiest links:")
            sorted_links = sorted(link_util.items(), key=lambda x: x[1], reverse=True)
            for (u, v), bps in sorted_links[:5]:
                print(f"     Link {u}→{v}: {bps/1e6:.2f} Mbps")
        
        print("\n7. Testing get_flow_distribution()...")
        flow_dist = db.get_flow_distribution()
        print(f"   ✓ Found {len(flow_dist)} src-dst pairs with active flows")
        if flow_dist:
            print("   Sample distribution:")
            for (src, dst), routes in list(flow_dist.items())[:3]:
                print(f"     {src}→{dst}: {sum(routes.values())} flows across routes {list(routes.keys())}")
        
        print("\n" + "=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)
        print("\nYou can now run:")
        print("  - python ecmp.py        (for ECMP algorithm)")
        print("  - python waterfilling.py (for Waterfilling algorithm)")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nTroubleshooting:")
        print("1. Check if PostgreSQL is running:")
        print("   sudo systemctl status postgresql")
        print("2. Check if database exists:")
        print("   psql -h 127.0.0.1 -U pgusr -l")
        print("3. Check if you can connect:")
        print("   psql -h 127.0.0.1 -U pgusr -d nextmini")
        print("4. Check if controller is running and has initialized the database")
        return False


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
