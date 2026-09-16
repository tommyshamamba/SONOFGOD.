# Kubernetes Demo Project

A practical microservices application demonstrating key Kubernetes concepts including Deployments, Services, ConfigMaps, Secrets, and Horizontal Pod Autoscaling.

## Architecture

- **Backend**: Node.js/Express API service
- **Frontend**: React single-page application served by Nginx
- **Kubernetes Resources**: Deployments, Services, ConfigMap, Secret, HPA

## Project Structure

```
kubernetes-demo/
├── backend/              # Node.js API service
│   ├── server.js
│   ├── package.json
│   └── Dockerfile
├── frontend/             # React frontend
│   ├── src/
│   ├── public/
│   ├── nginx.conf
│   ├── package.json
│   └── Dockerfile
├── k8s/                  # Kubernetes manifests
│   ├── backend-deployment.yaml
│   ├── backend-service.yaml
│   ├── frontend-deployment.yaml
│   ├── frontend-service.yaml
│   ├── configmap.yaml
│   ├── secret.yaml
│   └── hpa.yaml
├── docker-compose.yml    # Local development
└── README.md
```

## Prerequisites

- Docker and Docker Compose
- Kubernetes cluster (Minikube, Kind, or cloud provider)
- kubectl CLI tool

## Local Development with Docker Compose

1. **Start the application:**
```bash
docker-compose up --build
```

2. **Access the application:**
- Frontend: http://localhost
- Backend API: http://localhost:3000

3. **Stop the application:**
```bash
docker-compose down
```

## Kubernetes Deployment

### Step 1: Build Docker Images

Build images for your local cluster or push to a registry:

```bash
# Build backend image
docker build -t k8s-demo-backend:latest ./backend

# Build frontend image
docker build -t k8s-demo-frontend:latest ./frontend

# If using Minikube, load images into the cluster
minikube image load k8s-demo-backend:latest
minikube image load k8s-demo-frontend:latest
```

### Step 2: Create Kubernetes Resources

Apply all manifests:

```bash
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/backend-deployment.yaml
kubectl apply -f k8s/backend-service.yaml
kubectl apply -f k8s/frontend-deployment.yaml
kubectl apply -f k8s/frontend-service.yaml
kubectl apply -f k8s/hpa.yaml
```

Or apply all at once:

```bash
kubectl apply -f k8s/
```

### Step 3: Verify Deployment

Check pod status:
```bash
kubectl get pods
```

Check services:
```bash
kubectl get services
```

Check HPA status:
```bash
kubectl get hpa
```

### Step 4: Access the Application

**Minikube:**
```bash
minikube service frontend-service
```

**Other clusters:**
```bash
kubectl get service frontend-service
# Use the EXTERNAL-IP to access the application
```

## Kubernetes Concepts Demonstrated

### Deployment
- Manages replica sets for both frontend (2 replicas) and backend (3 replicas)
- Ensures desired state is maintained
- Supports rolling updates and rollbacks

### Service
- **ClusterIP** for backend (internal communication)
- **LoadBalancer** for frontend (external access)
- Provides stable network endpoints

### ConfigMap
- Stores non-sensitive configuration data
- Environment variables: NODE_ENV, APP_NAME, LOG_LEVEL, FEATURE_FLAGS
- Mounted as environment variables in pods

### Secret
- Stores sensitive data (API keys, passwords)
- Base64 encoded values
- Mounted as environment variables in pods

### Horizontal Pod Autoscaler (HPA)
- Automatically scales backend pods based on CPU/memory usage
- Min replicas: 2, Max replicas: 10
- CPU target: 50%, Memory target: 70%

### Health Checks
- **Liveness Probe**: Checks if container is running
- **Readiness Probe**: Checks if container is ready to serve traffic
- Both use `/health` endpoint for backend and `/` for frontend

### Resource Limits
- CPU and memory requests/limits defined for each container
- Prevents resource starvation and ensures fair scheduling

## API Endpoints

### Backend Service

- `GET /health` - Health check endpoint
- `GET /api/data` - Returns pod and node information
- `GET /api/config` - Returns ConfigMap data
- `GET /api/secret` - Returns masked Secret data

## Useful kubectl Commands

```bash
# View all resources
kubectl get all

# View pod logs
kubectl logs <pod-name>

# View pod details
kubectl describe pod <pod-name>

# Scale deployment manually
kubectl scale deployment backend-deployment --replicas=5

# View ConfigMap
kubectl get configmap app-config -o yaml

# View Secret (decoded)
kubectl get secret app-secret -o yaml

# Delete all resources
kubectl delete -f k8s/

# Port forward to access service locally
kubectl port-forward service/backend-service 3000:3000
```

## Updating Secrets

To update secret values:

```bash
# Encode new value
echo -n "new-api-key" | base64

# Edit secret
kubectl edit secret app-secret

# Or recreate secret
kubectl apply -f k8s/secret.yaml
```

## Rolling Updates

To update the application:

1. Build new image with updated tag
2. Update image in deployment:
```bash
kubectl set image deployment/backend-deployment backend=k8s-demo-backend:v2
```
3. Watch rollout status:
```bash
kubectl rollout status deployment/backend-deployment
```

## Rollback

To rollback to previous version:

```bash
kubectl rollout undo deployment/backend-deployment
```

## Troubleshooting

**Pods not starting:**
```bash
kubectl describe pod <pod-name>
kubectl logs <pod-name>
```

**Service not accessible:**
```bash
kubectl get endpoints
kubectl describe service <service-name>
```

**HPA not scaling:**
```bash
kubectl describe hpa backend-hpa
kubectl top pods
```

## Learning Exercises

1. **Scale manually**: Use `kubectl scale` to change replica counts
2. **Test HPA**: Generate load on backend and watch autoscaling
3. **Update ConfigMap**: Modify config and observe changes
4. **Simulate failure**: Delete a pod and watch self-healing
5. **Rolling update**: Deploy new version and observe zero-downtime update
6. **Resource limits**: Adjust requests/limits and observe scheduling

## Security Notes

- Change default secret values before production deployment
- Use image pull secrets for private registries
- Enable RBAC for cluster access control
- Use network policies to restrict pod communication
- Consider using external secret management (e.g., HashiCorp Vault)
