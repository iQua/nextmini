"""
This is an implementation of the waterfilling algorithm.
This algorithm splits streams among paths based on their relative capacity.
"""

from collections import defaultdict
from time import sleep

from common import Database


class Algorithm:
    def __init__(self, db_creds: dict, npaths: int):
        self.db = Database(db_creds)
        self.npaths = npaths

    def run(self, update_interval: int):
        rounds = 0
        route_caps = defaultdict(lambda: 2**32)
        route_expected = defaultdict(lambda: -1 * 2**32)
        route_budget = defaultdict(lambda: 2**32)
        expected_modifier = 0.8

        while True:
            routes_bps = self.db.get_newest_perroute()
            flows = [key for key in self.db.get_newest_perflow().keys()]
            # We want to first update the capacity for the routes and set the budgets.
            for src_id, dst_id in flows:
                for route_id in range(self.npaths):
                    bps = routes_bps[(src_id, dst_id, route_id)]
                    if (
                        route_expected[(src_id, dst_id, route_id)]
                        * expected_modifier
                        > bps
                    ):
                        # If the actual bps is less than expected, we set the capacity to the actual bps
                        route_caps[(src_id, dst_id, route_id)] = bps
                        print(
                            f"Round {rounds}: Route {route_id} has less bps than expected. Setting capacity to {bps}"
                        )
                    if route_caps[(src_id, dst_id, route_id)] < bps:
                        # If the acutal bps exceeds the capacity, we set the capacity to the acutal bps
                        route_caps[(src_id, dst_id, route_id)] = bps
                        print(
                            f"Round {rounds}: Route {route_id} exceeded capacity. Setting capacity to {bps}"
                        )
                    route_budget[(src_id, dst_id, route_id)] = (
                        route_caps[(src_id, dst_id, route_id)] - bps
                    )

            # loop through all the routes and reassign streams
            routes_streams = self.db.get_newest_routes_streams()
            stream_bps = self.db.get_newest_streambps()
            stream_assign = defaultdict(lambda: [])

            for src_id, dst_id in flows:
                # Loop through all the routes, and sort routes in each flow according to the budget avaialable
                routes = [
                    (route_id, route_budget[(src_id, dst_id, route_id)])
                    for route_id in range(self.npaths)
                ]
                routes.sort(key=lambda item: item[1])

                # Obtain ids for routes with the least and most budget
                min_id = routes[0][
                    0
                ]  # id of the route with the smallest budget
                max_id = routes[-1][
                    0
                ]  # id of the route with the largets budget

                # Find the stream with the smallest bps in the route with the smallest budget
                min_route_streams = [
                    (stream, stream_bps[src_id, dst_id, stream])
                    for stream in routes_streams[(src_id, dst_id, min_id)]
                ]
                min_route_streams.sort(key=lambda item: item[1])
                total_stream_bps = sum(
                    [stream[1] for stream in min_route_streams]
                )
                if len(min_route_streams) == 0:
                    print(f"Round {rounds}: Min route streams is empty")
                    continue
                (min_stream_id, min_stream_bps) = min_route_streams.pop(0)

                # Adjust the min bps in proportion to the actual route bps (which is route capacity - route budget)
                try:
                    min_stream_bps = (
                        min_stream_bps
                        / total_stream_bps
                        * (
                            route_caps[(src_id, dst_id, min_id)]
                            - route_budget[(src_id, dst_id, min_id)]
                        )
                    )
                except Exception:
                    print(
                        f"Round {rounds}: No streams to assign to route {min_id}"
                    )
                    continue

                # Check to see if moving the stream will result in higher overall min budget. If not,skip.
                if routes[-1][1] - min_stream_bps < routes[0][1]:
                    continue

                # Move the min stream from the route with the smallest budget to the route with the largest budget.
                min_route_streams = [stream[0] for stream in min_route_streams]
                max_route_streams = routes_streams[(src_id, dst_id, max_id)]
                max_route_streams.append(min_stream_id)

                # Update the route assignment for both routes
                stream_assign[(src_id, dst_id, min_id)].extend(
                    min_route_streams
                )
                stream_assign[(src_id, dst_id, max_id)].extend(
                    max_route_streams
                )

                # Update the expected bps for both routes
                route_expected[(src_id, dst_id, min_id)] = sum(
                    [
                        stream_bps[src_id, dst_id, stream]
                        for stream in min_route_streams
                    ]
                )
                route_expected[(src_id, dst_id, max_id)] = sum(
                    [
                        stream_bps[src_id, dst_id, stream]
                        for stream in max_route_streams
                    ]
                )

            # Install the route assignment.
            cursor = self.db.get_cursor()
            f_has_install = False
            for key, val in stream_assign.items():
                f_has_install = True
                (src_id, dst_id, route_id) = key
                print(f"Round {rounds}: Assigning {val} to route {route_id}")
                conns = [[int(s) for s in conn.split(":")] for conn in val]
                path = self.db.get_path_hops(src_id, dst_id, route_id)
                self.db.install_route(
                    cursor=cursor,
                    route_id=route_id,
                    src=src_id,
                    dst=dst_id,
                    path=path,
                    streams=conns,
                )
            # Sync to db.
            if f_has_install:
                print(
                    "Requested the controller to sync its database with the dataplane."
                )
                self.db.sync_db(cursor)
            sleep(update_interval)
            rounds += 1


if __name__ == "__main__":
    creds = {
        "user": "pgusr",
        "password": "pgpwrd",
        "host": "127.0.0.1",
        "port": "5432",
        "database": "strato",
    }
    alg = Algorithm(creds, 3)
    alg.run(2)
