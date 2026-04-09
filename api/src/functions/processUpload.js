const { app } = require('@azure/functions');
const { CosmosClient } = require("@azure/cosmos");
const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

const endpoint = process.env.COSMOS_ENDPOINT; 
const key = process.env.COSMOS_KEY;
const client = new CosmosClient({ endpoint, key });

app.eventGrid('processUpload', {
    handler: async (event, context) => {
        context.log(`Event Grid trigger fired for event: ${event.eventType}`);
        
        if (event.eventType === 'Microsoft.Storage.BlobCreated') {
            const blobUrl = event.data.url;
            context.log(`New file detected at: ${blobUrl}`);
            
            const filePath = path.join(__dirname, '../../data/amazon_sales_dataset.csv');
            const database = client.database("SalesDB");
            const container = database.container("SalesData");

            context.log("Starting Cosmos DB ingestion...");
            
            let count = 0;
            fs.createReadStream(filePath)
                .pipe(csv())
                .on('data', async (row) => {
                    const document = {
                        id: row['order_id'],
                        order_date: row['order_date'],
                        product_id: row['product_id'],
                        product_category: row['product_category'],
                        customer_region: row['customer_region'],
                        total_revenue: parseFloat(row['total_revenue']),
                        rating: parseFloat(row['rating'])
                    };

                    try {
                        await container.items.upsert(document);
                        count++;
                        if (count % 1000 === 0) context.log(`Uploaded ${count} rows...`);
                    } catch (err) {
                        context.error("Cosmos DB Insert Error:", err.message);
                    }
                })
                .on('end', () => {
                    context.log(`✅ Ingestion complete!`);
                });
        }
    }
});