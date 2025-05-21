import Dashboard from './dashboard'
import Chart from './chart'

export default function Home({ metrics }){
  return (
    <main className="p-8 max-h-screen m-12">   
      <div className="mb-4">
        <h1 className="text-4xl font-bold text-center">Strato Metrics Dashboard</h1>
      </div>
      <div className="p-8 max-h-screen">
          <div className="flex flex-col space-y-4 xl:space-y-0 xl:flex-row">
              <div className="flex-1">
                  <Chart metrics={metrics} />
              </div>
              <div className="flex-1">
                  <Dashboard metrics={metrics} />
              </div>
          </div>
      </div>
    </main>
  )
}

const dummyMetrics = []; // Used for testing purposes only
for (let i = 1; i <= 1000; i++){
  dummyMetrics.push({
    id: Math.ceil(Math.random() * 500),
    src_node_id: Math.ceil(Math.random() * 500),
    dst_node_id: Math.ceil(Math.random() * 500),
    flow_id: Math.ceil(Math.random() * 500),
    time_read: new Date().toISOString(),
    bps: Math.random() * 500,
  });
}

export async function getServerSideProps(){
  try {
      const res = await fetch('http://localhost:3000/api/metrics');
      const metrics = await res.json();
      return {
        props: { metrics: metrics },
      };
  } catch (error){
      console.error(error);
      return {
        props: { metrics: [] }, 
      };
  }
}