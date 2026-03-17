import { useState, useEffect } from 'react';
import { LineChart, Line, BarChart, Bar, PieChart, Pie, Cell, Treemap, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { DollarSign, ShoppingCart, TrendingUp, Star, Filter, Calendar, Download, FileText } from 'lucide-react';
import './App.css';

// --- MOCK DATA FALLBACK ---
const PIE_COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042'];

const mockDatabase = {
  kpis: { totalRevenue: 1254300, totalOrders: 50000, avgOrderValue: 25.08, avgRating: 4.6 },
  trend: [
    { month: 'Jan', revenue: 150000 }, { month: 'Feb', revenue: 180000 },
    { month: 'Mar', revenue: 170000 }, { month: 'Apr', revenue: 210000 },
    { month: 'May', revenue: 250000 }, { month: 'Jun', revenue: 294300 }
  ],
  categories: [
    { name: 'Electronics', value: 500000 }, { name: 'Apparel', value: 350000 },
    { name: 'Home & Kitchen', value: 254300 }, { name: 'Books', value: 150000 }
  ],
  topProducts: [
    { name: 'Kindle Paperwhite', size: 120000 }, { name: 'Echo Dot', size: 95000 },
    { name: 'Fleece Jacket', size: 80000 }, { name: 'Sony Headphones', size: 75000 },
    { name: 'Coffee Maker', size: 60000 }, { name: 'Atomic Habits', size: 45000 }
  ],
  transactions: [
    { id: 'ORD-9012', date: '2026-03-17', product: 'Sony Headphones', region: 'North America', amount: 199.99, status: 'Completed' },
    { id: 'ORD-9013', date: '2026-03-17', product: 'Atomic Habits', region: 'Europe', amount: 15.99, status: 'Completed' },
    { id: 'ORD-9014', date: '2026-03-16', product: 'Kindle Paperwhite', region: 'Asia', amount: 129.99, status: 'Pending' }
  ]
};

// --- CUSTOM TREEMAP RENDERER ---
const CustomizedTreemapContent = ({ root, depth, x, y, width, height, index, payload, colors, rank, name }) => {
  return (
    <g>
      <rect x={x} y={y} width={width} height={height} style={{ fill: depth < 2 ? PIE_COLORS[index % PIE_COLORS.length] : '#ffffff00', stroke: '#fff', strokeWidth: 2 }} />
      {width > 50 && height > 30 ? (
        <text x={x + width / 2} y={y + height / 2 + 5} textAnchor="middle" fill="#fff" fontSize={12} fontWeight="bold">{name}</text>
      ) : null}
    </g>
  );
};

// --- DASHBOARD COMPONENT ---
function App() {
  const [selectedRegion, setSelectedRegion] = useState('All');
  const [selectedTimeframe, setSelectedTimeframe] = useState('YTD');
  const [isLoading, setIsLoading] = useState(false);
  
  // THE FIX: We added dashboardData to state, initialized with our mock data so it doesn't crash on the first render!
  const [dashboardData, setDashboardData] = useState(mockDatabase);

  // --- REAL API CALL ---
  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      try {
        const response = await fetch(`/api/getDashboardData?region=${selectedRegion}&timeframe=${selectedTimeframe}`);
        
        if (!response.ok) throw new Error("Network response was not ok");
        
        const data = await response.json();
        setDashboardData(data); // This works now!
      } catch (error) {
        console.error("Failed to fetch data:", error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [selectedRegion, selectedTimeframe]);

  const handleDownloadCsv = () => {
    // We also updated this to pull from the REAL state data
    const data = dashboardData.transactions; 
    if (!data || !data.length) return;

    const headers = Object.keys(data[0]).join(',');
    const rows = data.map(row => Object.values(row).join(',')).join('\n');
    const csvContent = `${headers}\n${rows}`;

    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', `amazon_sales_${selectedRegion}_${selectedTimeframe}.csv`.replace(/\s+/g, '_').toLowerCase());
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleGenerateReport = () => {
    window.print();
  };

  return (
    <div className="dashboard-container" style={{ padding: '30px', fontFamily: 'sans-serif', backgroundColor: '#f4f7f6', minHeight: '100vh' }}>
      
      <style>{`
        @media print {
          body { background-color: white !important; }
          .no-print { display: none !important; }
          .dashboard-container { padding: 0 !important; }
          .chart-card { box-shadow: none !important; border: 1px solid #ddd; break-inside: avoid; }
        }
        .action-btn {
          display: flex; align-items: center; gap: 8px; padding: 10px 16px; border-radius: 6px; 
          border: none; font-weight: bold; cursor: pointer; transition: opacity 0.2s; font-size: 14px;
        }
        .action-btn:hover { opacity: 0.8; }
      `}</style>

      {/* HEADER & CONTROLS */}
      <header style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '30px', flexWrap: 'wrap', gap: '20px' }}>
        <div>
          <h1 style={{ margin: 0, color: '#2c3e50' }}>Amazon Sales Analytics</h1>
          <p style={{ margin: '5px 0 0 0', color: '#7f8c8d' }}>Interactive Performance Dashboard</p>
        </div>
        
        <div className="no-print" style={{ display: 'flex', gap: '15px', flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', backgroundColor: 'white', padding: '8px 16px', borderRadius: '8px', border: '1px solid #e1e8ed' }}>
            <Calendar size={18} color="#7f8c8d" />
            <select value={selectedTimeframe} onChange={(e) => setSelectedTimeframe(e.target.value)} style={{ padding: '4px', border: 'none', outline: 'none', cursor: 'pointer', fontWeight: 'bold', color: '#2c3e50' }}>
              <option value="7D">Last 7 Days</option>
              <option value="30D">Last 30 Days</option>
              <option value="YTD">Year to Date</option>
              <option value="ALL">All Time</option>
            </select>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '10px', backgroundColor: 'white', padding: '8px 16px', borderRadius: '8px', border: '1px solid #e1e8ed' }}>
            <Filter size={18} color="#7f8c8d" />
            <select value={selectedRegion} onChange={(e) => setSelectedRegion(e.target.value)} style={{ padding: '4px', border: 'none', outline: 'none', cursor: 'pointer', fontWeight: 'bold', color: '#2c3e50' }}>
              <option value="All">Global (All)</option>
              <option value="North America">North America</option>
              <option value="Europe">Europe</option>
              <option value="Asia">Asia</option>
            </select>
          </div>

          <div style={{ width: '1px', backgroundColor: '#ddd', margin: '0 5px' }}></div>
          
          <button onClick={handleDownloadCsv} className="action-btn" style={{ backgroundColor: '#fff', color: '#2c3e50', border: '1px solid #e1e8ed' }}>
            <Download size={16} /> Raw Data
          </button>
          
          <button onClick={handleGenerateReport} className="action-btn" style={{ backgroundColor: '#2980b9', color: '#fff' }}>
            <FileText size={16} /> PDF Report
          </button>
        </div>
      </header>

      {/* LOADING OVERLAY */}
      <div style={{ opacity: isLoading ? 0.4 : 1, transition: 'opacity 0.3s ease' }}>
        
        {/* KPI CARDS */}
        <div style={{ display: 'flex', gap: '20px', marginBottom: '25px', flexWrap: 'wrap' }}>
          <KpiCard title="Total Revenue" value={`$${(dashboardData.kpis.totalRevenue / 1000000).toFixed(2)}M`} icon={<DollarSign color="#2980b9" size={28}/>} />
          <KpiCard title="Total Orders" value={dashboardData.kpis.totalOrders.toLocaleString()} icon={<ShoppingCart color="#27ae60" size={28}/>} />
          <KpiCard title="Avg Order Value" value={`$${dashboardData.kpis.avgOrderValue}`} icon={<TrendingUp color="#8e44ad" size={28}/>} />
          <KpiCard title="Avg Rating" value={`${dashboardData.kpis.avgRating} / 5`} icon={<Star color="#f39c12" size={28}/>} />
        </div>

        {/* TOP ROW CHARTS */}
        <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '20px', marginBottom: '20px' }}>
          <ChartCard title="Revenue Trend">
            <ResponsiveContainer width="100%" height={280}>
              <LineChart data={dashboardData.trend}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#eee" />
                <XAxis dataKey="month" tickLine={false} axisLine={false} />
                <YAxis tickFormatter={(val) => `$${val/1000}k`} tickLine={false} axisLine={false} />
                <Tooltip formatter={(value) => `$${value.toLocaleString()}`} />
                <Line type="monotone" dataKey="revenue" stroke="#2980b9" strokeWidth={4} dot={{ r: 4 }} activeDot={{ r: 8 }} />
              </LineChart>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title="Sales by Category">
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie data={dashboardData.categories} cx="50%" cy="50%" innerRadius={60} outerRadius={90} paddingAngle={5} dataKey="value">
                  {dashboardData.categories.map((entry, index) => <Cell key={`cell-${index}`} fill={PIE_COLORS[index % PIE_COLORS.length]} />)}
                </Pie>
                <Tooltip formatter={(value) => `$${value.toLocaleString()}`} />
                <Legend verticalAlign="bottom" height={36}/>
              </PieChart>
            </ResponsiveContainer>
          </ChartCard>
        </div>

        {/* BOTTOM ROW CHARTS */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 2fr', gap: '20px' }}>
          <ChartCard title="Top Performing Products">
            <ResponsiveContainer width="100%" height={300}>
              <Treemap data={dashboardData.topProducts} dataKey="size" aspectRatio={4 / 3} stroke="#fff" fill="#8884d8" content={<CustomizedTreemapContent />}>
                <Tooltip formatter={(value) => `$${value.toLocaleString()}`} />
              </Treemap>
            </ResponsiveContainer>
          </ChartCard>

          <ChartCard title="Recent Transactions">
            <div style={{ overflowX: 'auto', maxHeight: '300px', overflowY: 'auto' }}>
              <table style={{ width: '100%', borderCollapse: 'collapse', textAlign: 'left', fontSize: '14px' }}>
                <thead style={{ position: 'sticky', top: 0, backgroundColor: 'white', zIndex: 1, borderBottom: '2px solid #eee' }}>
                  <tr>
                    <th style={{ padding: '12px' }}>Order ID</th>
                    <th style={{ padding: '12px' }}>Date</th>
                    <th style={{ padding: '12px' }}>Product</th>
                    <th style={{ padding: '12px' }}>Region</th>
                    <th style={{ padding: '12px' }}>Amount</th>
                    <th style={{ padding: '12px' }}>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {dashboardData.transactions.map((tx, idx) => (
                    <tr key={tx.id} style={{ borderBottom: '1px solid #eee', backgroundColor: idx % 2 === 0 ? '#fafafa' : 'white' }}>
                      <td style={{ padding: '12px', fontWeight: 'bold', color: '#2980b9' }}>{tx.id}</td>
                      <td style={{ padding: '12px' }}>{tx.date}</td>
                      <td style={{ padding: '12px' }}>{tx.product}</td>
                      <td style={{ padding: '12px' }}>{tx.region}</td>
                      <td style={{ padding: '12px', fontWeight: 'bold' }}>${parseFloat(tx.amount).toFixed(2)}</td>
                      <td style={{ padding: '12px' }}>
                        <span style={{ 
                          padding: '4px 10px', borderRadius: '12px', fontSize: '12px', fontWeight: 'bold',
                          backgroundColor: tx.status === 'Completed' ? '#e8f8f5' : tx.status === 'Pending' ? '#fef9e7' : '#fdedec',
                          color: tx.status === 'Completed' ? '#27ae60' : tx.status === 'Pending' ? '#f39c12' : '#e74c3c'
                        }}>
                          {tx.status}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </ChartCard>
        </div>

      </div>
    </div>
  );
}

function KpiCard({ title, value, icon }) {
  return (
    <div className="chart-card" style={{ flex: '1 1 200px', backgroundColor: 'white', padding: '24px', borderRadius: '12px', boxShadow: '0 4px 6px rgba(0,0,0,0.04)', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
      <div>
        <p style={{ margin: 0, color: '#7f8c8d', fontSize: '13px', fontWeight: 'bold', textTransform: 'uppercase' }}>{title}</p>
        <h2 style={{ margin: '8px 0 0 0', color: '#2c3e50', fontSize: '28px' }}>{value}</h2>
      </div>
      <div className="no-print" style={{ padding: '16px', backgroundColor: '#f8f9fa', borderRadius: '50%' }}>{icon}</div>
    </div>
  );
}

function ChartCard({ title, children }) {
  return (
    <div className="chart-card" style={{ backgroundColor: 'white', padding: '24px', borderRadius: '12px', boxShadow: '0 4px 6px rgba(0,0,0,0.04)' }}>
      <h3 style={{ margin: '0 0 20px 0', color: '#2c3e50', fontSize: '16px' }}>{title}</h3>
      {children}
    </div>
  );
}

export default App;