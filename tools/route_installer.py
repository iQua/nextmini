import psycopg2
import json
class Database: 
    def __init__(self):
        self.connection = psycopg2.connect(
            user="pgusr",
            password="pgpwrd",
            host="127.0.0.1",
            port="5432",
            database="strato"
        )
        self.print_buffer = []
    
    def install_route(self, route_id, src, dst, path, streams):
        streams = json.dumps(streams)
        cursor = self.connection.cursor()
        query = '''
        INSERT INTO "routes" (src_node_id, dst_node_id, route_id, hops, streams) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (src_node_id, dst_node_id, route_id) DO UPDATE SET hops = EXCLUDED.hops, streams = EXCLUDED.streams;
        '''
        cursor.execute(query, (src, dst, route_id, path, streams))
        self.connection.commit()
        cursor.close()
        
        
if __name__=="__main__":
    db = Database()
    db.install_route(
        route_id=1,
        src=1,
        dst=2,
        path=[1,3,2],
        streams=[]
    )
    db.install_route(
        route_id=0,
        src=1,
        dst=2,
        path=[1,3,2],
        streams=[[55928,5201]] 
    )