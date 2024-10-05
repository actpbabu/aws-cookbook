const express = require('express');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand, PutCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');
const NodeCache = require('node-cache');
require('dotenv').config();

const app = express();
const port = process.env.PORT || 3000;

// Configure AWS SDK
const client = new DynamoDBClient({ region: process.env.AWS_REGION });
client.config.credentials().then(
  (creds) => console.log('AWS credentials loaded successfully'),
  (err) => console.error('Error loading AWS credentials:', err)
);
const docClient = DynamoDBDocumentClient.from(client);

// Initialize cache
const cache = new NodeCache({ stdTTL: 600 }); // Cache for 10 minutes

const PREFETCH_PAGES = 3; // Number of additional pages to prefetch

app.use(express.json()); // Middleware to parse JSON bodies

app.get('/orders/:customerId', async (req, res) => {
  const customerId = req.params.customerId;
  const limit = parseInt(req.query.limit) || 10;
  const page = parseInt(req.query.page) || 1;

  try {
    const { items, lastEvaluatedKey } = await getCustomerPageItems(customerId, page, limit);

    // Prefetch next pages in the background
    if (lastEvaluatedKey) {
      prefetchNextPages(customerId, page, limit, lastEvaluatedKey);
    }

    res.json({
      orders: items,
      customerId,
      currentPage: page,
      itemsPerPage: limit,
      hasNextPage: !!lastEvaluatedKey,
      totalItems: items.length,
    });
  } catch (error) {
    console.error('Error fetching orders:', error);
    res.status(500).json({ 
      error: 'Internal Server Error', 
      details: error.message,
      stack: error.stack 
    });
  }
});

async function getCustomerPageItems(customerId, page, limit) {
  let lastEvaluatedKey;

  // Try to get the LastEvaluatedKey for the previous page from cache
  if (page > 1) {
    const cacheKey = `customer_${customerId}_lastEvaluatedKey_${page - 1}_${limit}`;
    lastEvaluatedKey = cache.get(cacheKey);
  }

  let items = [];
  let currentPage = 1;

  while (currentPage <= page) {
    const result = await queryCustomerOrders(customerId, limit, lastEvaluatedKey);
    items = result.Items;
    lastEvaluatedKey = result.LastEvaluatedKey;

    // Cache the LastEvaluatedKey for this page
    const cacheKey = `customer_${customerId}_lastEvaluatedKey_${currentPage}_${limit}`;
    cache.set(cacheKey, lastEvaluatedKey);

    if (!lastEvaluatedKey || currentPage === page) break;
    currentPage++;
  }

  return { items, lastEvaluatedKey };
}

async function prefetchNextPages(customerId, currentPage, limit, startKey) {
  let lastEvaluatedKey = startKey;

  for (let i = 1; i <= PREFETCH_PAGES; i++) {
    const nextPage = currentPage + i;
    if (!lastEvaluatedKey) break;

    const result = await queryCustomerOrders(customerId, limit, lastEvaluatedKey);
    const cacheKey = `customer_${customerId}_lastEvaluatedKey_${nextPage}_${limit}`;
    cache.set(cacheKey, result.LastEvaluatedKey);
    lastEvaluatedKey = result.LastEvaluatedKey;
  }
}

async function queryCustomerOrders(customerId, limit, exclusiveStartKey) {
  const params = {
    TableName: process.env.DYNAMODB_TABLE_NAME,
    IndexName: 'CustomerID-OrderDate-index',
    KeyConditionExpression: 'CustomerID = :customerId',
    ExpressionAttributeValues: {
      ':customerId': customerId,
    },
    Limit: limit,
    ExclusiveStartKey: exclusiveStartKey,
    ScanIndexForward: false, // This will sort by OrderDate in descending order
  };

  console.log('Query params:', JSON.stringify(params, null, 2));

  const command = new QueryCommand(params);
  const result = await docClient.send(command);
  
  console.log('Query result:', JSON.stringify(result, null, 2));
  console.log('Number of items returned:', result.Items.length);
  
  return result;
}

app.post('/orders', async (req, res) => {
  const { CustomerID, OrderNumber, OrderValue, OrderDate } = req.body;

  // Basic validation
  if (!CustomerID || !OrderNumber || !OrderValue || !OrderDate) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  const params = {
    TableName: process.env.DYNAMODB_TABLE_NAME,
    Item: {
      CustomerID,
      OrderNumber,
      OrderValue,
      OrderDate
    }
  };

  try {
    const command = new PutCommand(params);
    await docClient.send(command);

    // Invalidate cache for this customer
    invalidateCustomerCache(CustomerID);

    res.status(201).json({ message: 'Order created successfully', order: params.Item });
  } catch (error) {
    console.error('Error creating order:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

function invalidateCustomerCache(customerId) {
  const cacheKeys = cache.keys();
  const customerKeys = cacheKeys.filter(key => key.startsWith(`customer_${customerId}_lastEvaluatedKey_`));
  cache.del(customerKeys);
}

app.listen(port, () => {
  console.log(`Started on port ${port}`);
});

app.get('/all-orders', async (req, res) => {
  try {
    const params = {
      TableName: process.env.DYNAMODB_TABLE_NAME,
    };
    console.log(' params:', JSON.stringify(params, null, 2));
    
    const command = new ScanCommand(params);
    const result = await docClient.send(command);
    
    console.log(' result:', JSON.stringify(result, null, 2));
    
    res.json({
      items: result.Items,
      count: result.Count,
      scannedCount: result.ScannedCount,
      tableName: process.env.DYNAMODB_TABLE_NAME
    });
  } catch (error) {
    console.error(' all orders:', error);
    res.status(500).json({ error: 'Error', details: error.message });
  }
});

const { DescribeTableCommand } = require('@aws-sdk/client-dynamodb');

app.get('/table-info', async (req, res) => {
  try {
    const command = new DescribeTableCommand({ TableName: process.env.DYNAMODB_TABLE_NAME });
    const result = await client.send(command);
    res.json(result.Table);
  } catch (error) {
    console.error('table:', error);
    res.status(500).json({ error: 'error', details: error.message });
  }
});