# Kubernetes Deployment

Deploy Litestream with SQLite applications on Kubernetes.

## Deployment Patterns

### 1. Sidecar Container (Recommended)

Litestream runs alongside your application:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
spec:
  replicas: 1  # SQLite requires single writer
  selector:
    matchLabels:
      app: myapp
  template:
    metadata:
      labels:
        app: myapp
    spec:
      containers:
        - name: app
          image: myapp:latest
          volumeMounts:
            - name: data
              mountPath: /data

        - name: litestream
          image: litestream/litestream
          args:
            - replicate
          volumeMounts:
            - name: data
              mountPath: /data
            - name: config
              mountPath: /etc/litestream.yml
              subPath: litestream.yml
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: litestream-secrets
                  key: aws-access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: litestream-secrets
                  key: aws-secret-access-key

      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: myapp-data
        - name: config
          configMap:
            name: litestream-config
```

### 2. Init Container (Auto-Restore)

Restore database before application starts:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: myapp
spec:
  replicas: 1
  template:
    spec:
      initContainers:
        - name: restore
          image: litestream/litestream
          args:
            - restore
            - -if-db-not-exists
            - -if-replica-exists
            - -o
            - /data/app.db
            - s3://bucket/backup
          volumeMounts:
            - name: data
              mountPath: /data
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: litestream-secrets
                  key: aws-access-key-id
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: litestream-secrets
                  key: aws-secret-access-key

      containers:
        - name: app
          image: myapp:latest
          volumeMounts:
            - name: data
              mountPath: /data

        - name: litestream
          image: litestream/litestream
          args:
            - replicate
          volumeMounts:
            - name: data
              mountPath: /data
            - name: config
              mountPath: /etc/litestream.yml
              subPath: litestream.yml
```

## ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: litestream-config
data:
  litestream.yml: |
    dbs:
      - path: /data/app.db
        replica:
          url: s3://my-bucket/app-backup
          sync-interval: 1s
```

## Secrets

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: litestream-secrets
type: Opaque
stringData:
  aws-access-key-id: "AKIA..."
  aws-secret-access-key: "xxx"
```

Or create via kubectl:

```bash
kubectl create secret generic litestream-secrets \
  --from-literal=aws-access-key-id=AKIA... \
  --from-literal=aws-secret-access-key=xxx
```

## Persistent Volume Claim

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: myapp-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
  storageClassName: fast-ssd  # Use appropriate storage class
```

## StatefulSet (Alternative)

For stable network identity and storage:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: myapp
spec:
  serviceName: myapp
  replicas: 1
  selector:
    matchLabels:
      app: myapp
  template:
    metadata:
      labels:
        app: myapp
    spec:
      containers:
        - name: app
          image: myapp:latest
          volumeMounts:
            - name: data
              mountPath: /data

        - name: litestream
          image: litestream/litestream
          args:
            - replicate
          volumeMounts:
            - name: data
              mountPath: /data
            - name: config
              mountPath: /etc/litestream.yml
              subPath: litestream.yml

      volumes:
        - name: config
          configMap:
            name: litestream-config

  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 10Gi
```

## Service Account (GCS/Workload Identity)

For GKE with Workload Identity:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: litestream-sa
  annotations:
    iam.gke.io/gcp-service-account: litestream@project-id.iam.gserviceaccount.com
```

```yaml
spec:
  serviceAccountName: litestream-sa
  containers:
    - name: litestream
      image: litestream/litestream
```

## Health Probes

```yaml
containers:
  - name: litestream
    image: litestream/litestream
    args:
      - replicate
    livenessProbe:
      exec:
        command:
          - litestream
          - databases
      initialDelaySeconds: 10
      periodSeconds: 30
    readinessProbe:
      exec:
        command:
          - litestream
          - databases
      initialDelaySeconds: 5
      periodSeconds: 10
```

## Resource Limits

```yaml
containers:
  - name: litestream
    image: litestream/litestream
    resources:
      requests:
        cpu: 100m
        memory: 64Mi
      limits:
        cpu: 500m
        memory: 256Mi
```

## Metrics with ServiceMonitor

```yaml
apiVersion: v1
kind: Service
metadata:
  name: litestream-metrics
  labels:
    app: myapp
spec:
  ports:
    - port: 9090
      name: metrics
  selector:
    app: myapp
---
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: litestream
spec:
  selector:
    matchLabels:
      app: myapp
  endpoints:
    - port: metrics
```

Update Litestream config:

```yaml
# litestream.yml
addr: ":9090"

dbs:
  - path: /data/app.db
    replica:
      url: s3://bucket/backup
```

## Network Policies

Allow egress to S3:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: litestream-egress
spec:
  podSelector:
    matchLabels:
      app: myapp
  policyTypes:
    - Egress
  egress:
    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
      ports:
        - protocol: TCP
          port: 443
```

## Pod Disruption Budget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: myapp-pdb
spec:
  minAvailable: 0  # Allow disruption (single replica)
  selector:
    matchLabels:
      app: myapp
```

## Scaling Considerations

### Single Replica

SQLite requires single writer:

```yaml
spec:
  replicas: 1
```

### Anti-Affinity (Multi-AZ PVC)

If your PVC supports multi-AZ:

```yaml
spec:
  template:
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchLabels:
                    app: myapp
                topologyKey: topology.kubernetes.io/zone
```

## Troubleshooting

### Database not persisting
- Verify PVC is bound: `kubectl get pvc`
- Check pod events: `kubectl describe pod <pod>`

### Restore failing
- Check init container logs: `kubectl logs <pod> -c restore`
- Verify secrets are correct: `kubectl get secret litestream-secrets -o yaml`

### Litestream not starting
- Check sidecar logs: `kubectl logs <pod> -c litestream`
- Verify ConfigMap mounted: `kubectl exec <pod> -c litestream -- cat /etc/litestream.yml`

## See Also

- [Docker Deployment](docker.md)
- [GCS Configuration](../configuration/gcs.md)
- [S3 Configuration](../configuration/s3.md)
