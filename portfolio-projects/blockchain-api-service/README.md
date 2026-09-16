# Blockchain API Service

A production-ready blockchain API service that enables developers and businesses to easily integrate blockchain functionality without dealing with infrastructure complexity. This is a real infrastructure solution with monetization potential.

## Business Model

This service enables others to use blockchain technology by providing:
- **Simple REST API** for blockchain operations
- **Multi-chain support** (Ethereum, Polygon, Arbitrum, Optimism)
- **API key authentication** with rate limiting
- **Developer dashboard** for key management
- **Usage analytics** and monitoring

**Revenue Streams:**
- Tiered API pricing (free, pro, enterprise)
- Usage-based billing (per request)
- Premium features (dedicated nodes, priority support)
- White-label solutions for enterprises

## Features

- **Multi-chain support**: Ethereum, Polygon, Arbitrum, Optimism
- **RESTful API**: Simple HTTP endpoints for blockchain operations
- **Authentication**: JWT-based auth with API key management
- **Rate limiting**: Redis-based rate limiting per API key
- **Developer dashboard**: React-based UI for key management
- **Kubernetes-ready**: Complete K8s manifests with HPA
- **Docker support**: Docker Compose for local development

## Project Structure

```
blockchain-api-service/
├── backend/              # Node.js API service
│   ├── server.js
│   ├── package.json
│   ├── Dockerfile
│   └── .env.example
├── frontend/             # React dashboard
│   ├── src/
│   ├── public/
│   ├── package.json
│   └── Dockerfile
├── k8s/                  # Kubernetes manifests
│   ├── backend-deployment.yaml
│   ├── backend-service.yaml
│   ├── frontend-deployment.yaml
│   ├── frontend-service.yaml
│   ├── redis-deployment.yaml
│   ├── redis-service.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   └── hpa.yaml
├── docker-compose.yml
└── README.md
```

## API Endpoints

### Authentication
- `POST /api/auth/register` - Register new user
- `POST /api/auth/login` - Login user
- `POST /api/keys` - Create API key
- `GET /api/keys` - List API keys
- `DELETE /api/keys/:keyId` - Revoke API key

### Blockchain Operations
- `GET /api/v1/:chain/balance/:address` - Get wallet balance
- `GET /api/v1/:chain/nonce/:address` - Get transaction count
- `GET /api/v1/:chain/block/:blockNumber` - Get block information
- `GET /api/v1/:chain/transaction/:txHash` - Get transaction details
- `GET /api/v1/:chain/gas-price` - Get current gas price
- `POST /api/v1/:chain/broadcast` - Broadcast signed transaction
- `POST /api/v1/:chain/estimate-gas` - Estimate gas for transaction
- `GET /api/v1/chains` - List supported chains
- `GET /api/v1/usage` - Get API usage statistics

## Quick Start

### Local Development with Docker Compose

1. **Clone and navigate:**
```bash
cd C:\Users\USER\CascadeProjects\blockchain-api-service
```

2. **Start services:**
```bash
docker-compose up --build
```

3. **Access the application:**
- Dashboard: http://localhost
- API: http://localhost:3000

4. **Register and create API key:**
- Open dashboard in browser
- Register account
- Create API key
- Test API endpoints

### Manual Setup

**Backend:**
```bash
cd backend
npm install
cp .env.example .env
# Edit .env with your configuration
npm run dev
```

**Frontend:**
```bash
cd frontend
npm install
npm start
```

## Kubernetes Deployment

### Prerequisites
- Kubernetes cluster (Minikube, Kind, or cloud provider)
- kubectl CLI
- Docker registry access

### Build Images

```bash
# Build backend image
docker build -t blockchain-api-backend:latest ./backend

# Build frontend image
docker build -t blockchain-api-frontend:latest ./frontend

# If using Minikube
minikube image load blockchain-api-backend:latest
minikube image load blockchain-api-frontend:latest
```

### Deploy to Cluster

```bash
# Apply all manifests
kubectl apply -f k8s/

# Or apply individually
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/redis-deployment.yaml
kubectl apply -f k8s/redis-service.yaml
kubectl apply -f k8s/backend-deployment.yaml
kubectl apply -f k8s/backend-service.yaml
kubectl apply -f k8s/frontend-deployment.yaml
kubectl apply -f k8s/frontend-service.yaml
kubectl apply -f k8s/hpa.yaml
```

### Verify Deployment

```bash
# Check pods
kubectl get pods

# Check services
kubectl get services

# Check HPA
kubectl get hpa

# Access frontend (Minikube)
minikube service blockchain-api-frontend
```

## API Usage Examples

### Get Balance

```bash
curl -X GET "http://localhost:3000/api/v1/ethereum/balance/0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb" \
  -H "X-API-Key: your-api-key-here"
```

Response:
```json
{
  "chain": "ethereum",
  "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
  "balance": "1.234567890123456789",
  "wei": "1234567890123456789"
}
```

### Get Transaction

```bash
curl -X GET "http://localhost:3000/api/v1/ethereum/transaction/0x123...abc" \
  -H "X-API-Key: your-api-key-here"
```

### Broadcast Transaction

```bash
curl -X POST "http://localhost:3000/api/v1/ethereum/broadcast" \
  -H "X-API-Key: your-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{"signedTx": "0x..."}'
```

### Estimate Gas

```bash
curl -X POST "http://localhost:3000/api/v1/ethereum/estimate-gas" \
  -H "X-API-Key: your-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{
    "to": "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
    "from": "0x123...",
    "value": "0.1",
    "data": "0x..."
  }'
```

## Configuration

### Environment Variables

**Backend:**
- `PORT` - Server port (default: 3000)
- `JWT_SECRET` - JWT signing secret
- `REDIS_URL` - Redis connection URL
- `ETH_RPC_URL` - Ethereum RPC endpoint
- `POLYGON_RPC_URL` - Polygon RPC endpoint
- `ARBITRUM_RPC_URL` - Arbitrum RPC endpoint
- `OPTIMISM_RPC_URL` - Optimism RPC endpoint

**Frontend:**
- `REACT_APP_API_URL` - Backend API URL

### RPC Endpoints

For production, use your own nodes or premium services:
- **Infura**: https://infura.io
- **Alchemy**: https://www.alchemy.com
- **QuickNode**: https://www.quicknode.com
- **Ankr**: https://www.ankr.com

## Monetization Strategy

### Tiered Pricing

**Free Tier:**
- 1,000 requests/month
- Rate limit: 10 requests/minute
- Community support
- Public RPC endpoints

**Pro Tier ($49/month):**
- 100,000 requests/month
- Rate limit: 100 requests/minute
- Priority support
- Dedicated endpoints

**Enterprise ($499/month):**
- Unlimited requests
- Custom rate limits
- Dedicated support
- Private nodes
- SLA guarantees

### Usage-Based Pricing

Charge per request beyond included limits:
- $0.001 per additional request
- Volume discounts for high usage

### Premium Features

- Dedicated node access: $200/month
- Historical data access: $100/month
- Webhook notifications: $50/month
- Custom chain support: Custom pricing

## Scaling Considerations

### Horizontal Scaling
- Backend pods auto-scale based on CPU/memory (HPA configured)
- Redis for distributed rate limiting
- Load balancer for frontend

### Vertical Scaling
- Adjust resource requests/limits in deployments
- Use larger instance types for high load

### Caching
- Redis for rate limiting and response caching
- Consider CDN for static frontend assets

### Monitoring
- Add Prometheus metrics
- Set up Grafana dashboards
- Implement alerting

## Security Best Practices

1. **Use production RPC endpoints** - Don't rely on public nodes
2. **Rotate JWT secrets** regularly
3. **Implement proper database** - Replace in-memory storage
4. **Add HTTPS** - Use TLS certificates in production
5. **Rate limiting** - Already implemented with Redis
6. **Input validation** - Add comprehensive validation
7. **Audit logging** - Log all API calls for compliance
8. **IP whitelisting** - Add for enterprise customers

## Roadmap

### Phase 1 - MVP (Current)
- Basic blockchain operations
- Multi-chain support
- API key management
- Developer dashboard

### Phase 2 - Production
- Database integration (PostgreSQL)
- Enhanced authentication (OAuth, 2FA)
- Webhook support
- Historical data access

### Phase 3 - Enterprise
- Dedicated nodes
- Custom chain support
- White-label solutions
- Advanced analytics

### Phase 4 - Ecosystem
- SDK libraries (JS, Python, Go)
- Plugin system
- Marketplace for extensions
- Partner integrations

## Troubleshooting

**Redis connection failed:**
```bash
# Check Redis pod
kubectl logs blockchain-api-redis-xxxxx

# Check Redis service
kubectl get service blockchain-api-redis
```

**RPC endpoint errors:**
- Verify RPC URLs in ConfigMap
- Check node health
- Consider using premium RPC services

**Rate limit errors:**
- Check Redis connection
- Verify rate limiter configuration
- Review API key usage

## Support

For issues and questions:
- Check documentation
- Review API logs
- Test with public RPC endpoints first

## License

Proprietary - All rights reserved

## Acknowledgments

Built with:
- Express.js
- Ethers.js
- React
- Kubernetes
- Redis
