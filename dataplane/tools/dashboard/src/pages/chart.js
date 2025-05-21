const Chart = ({ metrics }) => {
    const pairBpsTotals = metrics.reduce((dictionary, metric) => {
      const key = `${metric.src_node_id}-${metric.dst_node_id}`;
      dictionary[key] = (dictionary[key] || 0) + metric.bps;
      return dictionary;
    }, {});

    const sortedPairBpsTotals = Object.entries(pairBpsTotals)
      .sort((i, j) => j[1] - i[1])
      .slice(0, 20);

    return (
        <div className="w-full scale-9"> {/* Need to be scaled down */}
            <h2 className="mb-4 text-2xl font-semibold text-gray-200 text-center">Total BPS by Source-Destination Pair</h2>
            <div className="flex flex-col space-y-3">

                {sortedPairBpsTotals.map(([pair, totalBps], index) => (
                <div key={index} className="flex items-center space-x-2">
                    <div className="font-medium w-28 text-right mr-4 text-gray-200">{pair}</div>
                    <div className="relative h-4 flex-grow bg-gray-200 rounded">
                        <div className="absolute h-full bg-blue-600 rounded"
                            style={{ width: `${totalBps / sortedPairBpsTotals[0][1] * 100}%` }}>
                            <div className="absolute top-0 left-1/2 transform -translate-x-1/2 text-white text-xs">
                                {totalBps.toLocaleString()}
                            </div>
                        </div>
                    </div>
                </div>
                ))}
                
            </div>
        </div>
    );
};

export default Chart;