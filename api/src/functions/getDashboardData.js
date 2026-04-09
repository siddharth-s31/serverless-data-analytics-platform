const { app } = require('@azure/functions');
const { CosmosClient } = require("@azure/cosmos");

const endpoint = process.env.COSMOS_ENDPOINT; 
const key = process.env.COSMOS_KEY;
const client = new CosmosClient({ endpoint, key });

app.http('getDashboardData', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        try {
            const region = request.query.get('region') || 'All';
            const timeframe = request.query.get('timeframe') || 'YTD';

            const database = client.database("SalesDB");
            const container = database.container("SalesData");

            // Query Cosmos DB
            let querySpec = { query: "SELECT * FROM c" };
            if (region !== 'All') {
                querySpec = {
                    query: "SELECT * FROM c WHERE c.customer_region = @region",
                    parameters: [{ name: "@region", value: region }]
                };
            }

            const { resources: filteredData } = await container.items.query(querySpec).fetchAll();

            // Aggregate Data
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
                if (rating > 0) { totalRating += rating; ratingCount++; }

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
                categories: Object.keys(categoryMap).map(key => ({ name: key, value: categoryMap[key] })).sort((a, b) => b.value - a.value).slice(0, 4),
                topProducts: Object.keys(productMap).map(key => ({ name: key, size: productMap[key] })).sort((a, b) => b.size - a.size).slice(0, 6)
            };

            return { jsonBody: responsePayload };

        } catch (error) {
            context.error("Cosmos DB Query Error:", error);
            return { status: 500, jsonBody: { error: "Database Connection Failed" } };
        }
    }
});