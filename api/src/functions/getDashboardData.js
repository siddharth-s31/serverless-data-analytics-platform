const { app } = require('@azure/functions');
const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');


let cachedData = [];

const loadData = () => {
    return new Promise((resolve, reject) => {
        if (cachedData.length > 0) return resolve(cachedData);
        
        const results = [];
        const filePath = path.join(__dirname, '../../data/amazon_sales_dataset.csv');
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => {
                cachedData = results;
                resolve(results);
            })
            .on('error', reject);
    });
};

app.http('getDashboardData', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        try {
            const region = request.query.get('region') || 'All';
            const timeframe = request.query.get('timeframe') || 'YTD';

            const allData = await loadData();

           
        // 3. Filter the Data
            let filteredData = allData;
            
            // --- A. Apply Region Filter ---
            if (region !== 'All') {
                filteredData = filteredData.filter(row => row['customer_region'] === region);
            }

            // --- B. Apply Timeframe Filter ---
            if (timeframe !== 'ALL' && filteredData.length > 0) {
                // 1. Find the "latest" date in the currently filtered dataset to act as "Today"
                const maxDateStr = filteredData.reduce((latest, row) => 
                    (row['order_date'] > latest ? row['order_date'] : latest), 
                    filteredData[0]['order_date']
                );
                const maxDate = new Date(maxDateStr);

                // 2. Calculate our cutoff threshold based on the dropdown choice
                let cutoffDate = new Date(maxDate);
                
                if (timeframe === '7D') {
                    cutoffDate.setDate(maxDate.getDate() - 7);
                } else if (timeframe === '30D') {
                    cutoffDate.setDate(maxDate.getDate() - 30);
                } else if (timeframe === 'YTD') {
                    // Set to January 1st of whatever year the maxDate is in
                    cutoffDate = new Date(maxDate.getFullYear(), 0, 1);
                }

                // 3. Actually filter the rows!
                filteredData = filteredData.filter(row => {
                    const rowDate = new Date(row['order_date']);
                    return rowDate >= cutoffDate && rowDate <= maxDate;
                });
            }

           
            let totalRevenue = 0;
            let totalRating = 0;
            let ratingCount = 0;
            let categoryMap = {};
            let productMap = {};

            filteredData.forEach(row => {
              
                const revenue = parseFloat(row['total_revenue'] || 0);
                const rating = parseFloat(row['rating'] || 0);
                const category = row['product_category'] || 'Unknown';
                
                const product = `Item #${row['product_id'] || 'Unknown'}`;

                totalRevenue += revenue;
                
                if (rating > 0) {
                    totalRating += rating;
                    ratingCount++;
                }

                categoryMap[category] = (categoryMap[category] || 0) + revenue;
                productMap[product] = (productMap[product] || 0) + revenue;
            });

            const responsePayload = {
                kpis: {
                    totalRevenue: totalRevenue,
                    totalOrders: filteredData.length,
                    avgOrderValue: filteredData.length > 0 ? (totalRevenue / filteredData.length).toFixed(2) : 0,
                    avgRating: ratingCount > 0 ? (totalRating / ratingCount).toFixed(1) : 0
                },
                categories: Object.keys(categoryMap).map(key => ({
                    name: key,
                    value: categoryMap[key]
                })).sort((a, b) => b.value - a.value).slice(0, 4),
                
                topProducts: Object.keys(productMap).map(key => ({
                    name: key,
                    size: productMap[key]
                })).sort((a, b) => b.size - a.size).slice(0, 6),
                
                transactions: filteredData.slice(0, 10).map((row, index) => ({
                    id: row['order_id'] || `ORD-${index}`,
                    date: row['order_date'],
                    product: `Item #${row['product_id']}`,
                    region: row['customer_region'],
                    amount: parseFloat(row['total_revenue'] || 0),
                    status: 'Completed'
                })),

                trend: [
                    { month: 'Jan', revenue: totalRevenue * 0.1 },
                    { month: 'Feb', revenue: totalRevenue * 0.15 },
                    { month: 'Mar', revenue: totalRevenue * 0.2 },
                    { month: 'Apr', revenue: totalRevenue * 0.25 },
                    { month: 'May', revenue: totalRevenue * 0.3 }
                ]
            };

            return { jsonBody: responsePayload };

        } catch (error) {
            context.error("Error processing request:", error);
            return { status: 500, jsonBody: { error: "Internal Server Error" } };
        }
    }
});