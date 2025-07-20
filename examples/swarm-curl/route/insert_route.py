#!/usr/bin/env python3
"""
Route Manager for Docker Swarm Services
Extracts client and server IPs and inserts routes into the database
"""

import docker
import psycopg2
from typing import Dict, Optional, List

# Constants
INTERMEDIATE_HOPS = [1, 2, 3]
SERVICE_NAMES = ['client', 'server']

class RouteManager:
    def __init__(self):
        self.docker_client = docker.from_env()
        self.db_connection = psycopg2.connect(
            user="pgusr",
            password="pgpwrd",
            host="206.12.91.13", # TODO: change to the controller/database instance IP.
            port="5432",
            database="nextmini"
        )
        self.db_connection.autocommit = True

    def get_service_ip(self, service_name: str) -> Optional[str]:
        """Get virtual IP for a specific service"""
        service = self.docker_client.services.get(service_name)
        virtual_ips = service.attrs.get('Endpoint', {}).get('VirtualIPs', [])
        
        if virtual_ips:
            return virtual_ips[0].get('Addr', '').split('/')[0]
        return None

    def get_all_service_ips(self) -> Dict[str, str]:
        """Get IPs for all required services"""
        service_ips = {}
        for service_name in SERVICE_NAMES:
            ip = self.get_service_ip(service_name)
            if ip:
                service_ips[service_name] = ip
        return service_ips

    def ip_to_node_id(self, service_name: str) -> Optional[int]:
        """Convert service IP to node ID by extracting from IP address"""
        # Get the actual IP for the service
        service_ip = self.get_service_ip(service_name)
        if not service_ip:
            return None
            
        # Extract node ID from IP (assuming 172.16.8.X format)
        # The node ID is the last octet of the IP
        try:
            ip_parts = service_ip.split('.')
            if len(ip_parts) == 4 and ip_parts[0] == '172' and ip_parts[1] == '16' and ip_parts[2] == '8':
                node_id = int(ip_parts[3])
                return node_id
        except (ValueError, IndexError):
            pass
            
        return None

    def insert_route(self, src_node_id: int, dst_node_id: int, route_path: List[int]):
        """Insert a route into the database"""
        cursor = self.db_connection.cursor()
        query = """
            INSERT INTO routes (src_node_id, dst_node_id, route)
            VALUES (%s, %s, %s)
            ON CONFLICT (src_node_id, dst_node_id, route) DO NOTHING
        """
        cursor.execute(query, (src_node_id, dst_node_id, route_path))
        cursor.close()

    def create_routes(self, service_ips: Dict[str, str]):
        """Create routes between client and server services"""
        client_node_id = self.ip_to_node_id('client')
        server_node_id = self.ip_to_node_id('server')

        # Route from client to server: [client, 1, 2, 3, server]
        client_to_server_path = [client_node_id] + INTERMEDIATE_HOPS + [server_node_id]
        self.insert_route(client_node_id, server_node_id, client_to_server_path)

        # Route from server to client: [server, 3, 2, 1, client]
        server_to_client_path = [server_node_id] + INTERMEDIATE_HOPS[::-1] + [client_node_id]
        self.insert_route(server_node_id, client_node_id, server_to_client_path)

    def run(self):
        """Main execution function"""
        service_ips = self.get_all_service_ips()
        self.create_routes(service_ips)
        self.db_connection.close()

if __name__ == "__main__":
    manager = RouteManager()
    manager.run()