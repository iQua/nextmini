import { useState } from 'react';

const TableHeader = ({ children }) => (
  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
    {children}
  </th>
);

const TableCell = ({ children }) => (
  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
    {children}
  </td>
);

const Pages = ({ totalPages, currentPage, onPageChange }) => {
  const [startingPage, setStartingPage] = useState(1);
  const maxPageNumbersToShow = 5;

  const handleNextPage = () => {
    setStartingPage(prev => Math.min(prev + maxPageNumbersToShow, totalPages - maxPageNumbersToShow + 1));
  };

  const handlePrevPage = () => {
    setStartingPage(prev => Math.max(prev - maxPageNumbersToShow, 1));
  };

  const visiblePages = Array.from(
    { length: Math.min(maxPageNumbersToShow, totalPages - startingPage + 1) },
    (_, idx) => startingPage + idx
  );

  return (
    <nav className="block">
      <ul className="flex pl-0 list-none justify-center">
        {startingPage > 1 && (
          <li>
            <button onClick={handlePrevPage} className="text-xs px-3 py-1 mx-1 rounded bg-white text-blue-500">
              &laquo;
            </button>
          </li>
        )}

        {visiblePages.map(pageNumber => (
          <li key={pageNumber}>
            <button
              onClick={() => onPageChange(pageNumber)}
              className={`text-xs px-3 py-1 mx-1 rounded ${
                pageNumber === currentPage ? 'bg-blue-500 text-white' : 'bg-white text-blue-500'
              }`}
            >
              {pageNumber}
            </button>
          </li>
        ))}

        {startingPage + maxPageNumbersToShow - 1 < totalPages && (
          <li>
            <button onClick={handleNextPage} className="text-xs px-3 py-1 mx-1 rounded bg-white text-blue-500">
              &raquo;
            </button>
          </li>
        )}
      </ul>
    </nav>
  );
};

const Dashboard = ({ metrics }) => {
  const itemsPerPage = 15;
  const [currentPage, setCurrentPage] = useState(1);

  const totalItems = metrics ? metrics.length : 0;
  const totalPages = Math.ceil(totalItems / itemsPerPage);
  const currentMetrics = metrics ? metrics.slice((currentPage - 1) * itemsPerPage, currentPage * itemsPerPage) : [];

  return (
    <div className="flex flex-col w-full ml-12 scale-90"> {/* Need to be scaled down */}
      <div className="-my-2 overflow-x-auto sm:-mx-6 lg:-mx-8">
        <div className="py-2 align-middle inline-block min-w-full sm:px-6 lg:px-8">
          <div className="shadow overflow-hidden border-b border-gray-200 sm:rounded-lg">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <TableHeader>ID</TableHeader>
                  <TableHeader>Source Node ID</TableHeader>
                  <TableHeader>Destination Node ID</TableHeader>
                  <TableHeader>Flow ID</TableHeader>
                  <TableHeader>Time Read</TableHeader>
                  <TableHeader>bps</TableHeader>
                </tr>
              </thead>

              <tbody className="bg-white divide-y divide-gray-200">
                {currentMetrics.map((metric, index) => (
                  <tr key={metric.id} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                    <TableCell>{metric.id || 'N/A'}</TableCell>
                    <TableCell>{metric.src_node_id || 'N/A'}</TableCell>
                    <TableCell>{metric.dst_node_id || 'N/A'}</TableCell>
                    <TableCell>{metric.flow_id || 'N/A'}</TableCell>
                    <TableCell>{metric.time_read || 'N/A'}</TableCell>
                    <TableCell>{metric.bps || 'N/A'}</TableCell>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
      
      <div className="p-4">
        <Pages
          totalPages={totalPages}
          currentPage={currentPage}
          onPageChange={setCurrentPage}
        />
      </div>
    </div>
  );
};

export default Dashboard;
