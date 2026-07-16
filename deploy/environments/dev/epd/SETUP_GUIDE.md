# EPP Router Setup Guide - sim-program-aware Plugin

Complete guide to set up EPP router with sim-program-aware-fairness plugin routing to 4 sim vLLM pods.

## 📋 Prerequisites

### Required Tools
- ✅ Kubernetes cluster (Rancher Desktop, kind, or similar)
- ✅ `kubectl` configured and working
- ✅ `docker` or `podman` for building images
- ✅ `git` for cloning repositories

### Required Images
- ✅ `epp-scheduler:local` - EPP router image
- ✅ `llm-d-inference-sim:v0.9.0` - vLLM simulator image

### Verify Prerequisites
```bash
# Check kubectl
kubectl version --client

# Check cluster access
kubectl get nodes

# Check if images exist
docker images | grep -E "epp-scheduler|llm-d-inference-sim"
```

---

## 🚀 Step-by-Step Setup

### Step 1: Deploy 4 sim vLLM Pods

**File**: `config/manifests/vllm/sim-deployment.yaml` (should already exist)

```bash
# Navigate to project root
cd ~/Documents/Pradnya/llm-d-inference-scheduler

# Deploy 4 sim vLLM pods
kubectl apply -f config/manifests/vllm/sim-deployment.yaml

# Verify pods are running
kubectl get pods -l app=vllm-qwen3-32b
```

**Expected Output**:
```
NAME                              READY   STATUS    RESTARTS   AGE
vllm-qwen3-32b-75c8f78b59-2rdmg   1/1     Running   0          5m
vllm-qwen3-32b-75c8f78b59-l8rch   1/1     Running   0          5m
vllm-qwen3-32b-75c8f78b59-v7nd4   1/1     Running   0          5m
vllm-qwen3-32b-75c8f78b59-zb4g8   1/1     Running   0          5m
```

---

### Step 2: Create sim-program-aware Configuration

**File**: `deploy/environments/dev/epd/sim-program-aware-config.yaml`

```yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
plugins:
- type: program-aware-fairness
- type: queue-scorer
- type: max-score-picker
- type: single-profile-handler

featureGates:
- flowControl

flowControl:
  defaultPriorityBand:
    fairnessPolicyRef: program-aware-fairness

schedulingProfiles:
- name: default
  plugins:
  - pluginRef: queue-scorer
  - pluginRef: max-score-picker
```

**Create the file**:
```bash
cd deploy/environments/dev/epd

cat > sim-program-aware-config.yaml <<'EOF'
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: EndpointPickerConfig
plugins:
- type: program-aware-fairness
- type: queue-scorer
- type: max-score-picker
- type: single-profile-handler

featureGates:
- flowControl

flowControl:
  defaultPriorityBand:
    fairnessPolicyRef: program-aware-fairness

schedulingProfiles:
- name: default
  plugins:
  - pluginRef: queue-scorer
  - pluginRef: max-score-picker
EOF
```

---

### Step 3: Create Patch Files

#### 3.1 Remove vllm-render Container

**File**: `deploy/environments/dev/epd/patch-remove-render.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${EPP_NAME}
spec:
  template:
    spec:
      containers:
      - name: vllm-render
        $patch: delete
      volumes:
      - name: model-cache
        $patch: delete
```

**Create the file**:
```bash
cat > patch-remove-render.yaml <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${EPP_NAME}
spec:
  template:
    spec:
      containers:
      - name: vllm-render
        $patch: delete
      volumes:
      - name: model-cache
        $patch: delete
EOF
```

#### 3.2 Add Pod Environment Variables

**File**: `deploy/environments/dev/epd/patch-add-pod-env.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${EPP_NAME}
spec:
  template:
    spec:
      containers:
      - name: epp
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
```

**Create the file**:
```bash
cat > patch-add-pod-env.yaml <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${EPP_NAME}
spec:
  template:
    spec:
      containers:
      - name: epp
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
EOF
```

---

### Step 4: Update Kustomization File

**File**: `deploy/environments/dev/epd/kustomization.yaml`

**Key Changes**:
1. Add patch files to patches list
2. Change `serviceAccountName` from `default` to `inference-gateway`
3. Remove `vllm-render` container from inline patch

**Complete kustomization.yaml**:
```yaml
# ------------------------------------------------------------------------------
# EPD (No Disaggregation) Development Environment - Day 1 Sandbox Setup
# ------------------------------------------------------------------------------
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
- ../../../components/vllm-decode/
- ../../../components/inference-gateway/

configMapGenerator:
- name: epp-config
  files:
  - epp-config.yaml=sim-program-aware-config.yaml

patches:
- path: patch-decode.yaml
- path: patch-remove-render.yaml
- path: patch-add-pod-env.yaml
- target:
    kind: Deployment
    name: \${EPP_NAME} 
  patch: |
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: inference-gateway
    spec:
      template:
        spec:
          serviceAccountName: inference-gateway
          containers:
          - name: epp
            image: epp-scheduler:local
            imagePullPolicy: Never
            args:
            - --endpoint-selector
            - "app=vllm-qwen3-32b"
            - --endpoint-target-ports
            - "8000"
            - --v
            - "4"
            - --zap-encoder
            - "json"
            - --grpc-port
            - "9002"
            - --grpc-health-port
            - "9003"
            - --config-file
            - "/etc/epp/epp-config.yaml"
            - --metrics-endpoint-auth=false
```

**Apply the changes**:
```bash
# Backup original
cp kustomization.yaml kustomization.yaml.backup

# Edit the file to match above
# Key changes:
# 1. Line 17-19: Add patch files
# 2. Line 32: Change serviceAccountName to inference-gateway
# 3. Remove lines 53-56: vllm-render container
```

---

### Step 5: Create Deployment Script

**File**: `deploy/environments/dev/epd/deploy-epp-router.sh`

```bash
#!/usr/bin/env bash
# Deploy EPP Router with sim-program-aware plugin for 4 sim vllm pods

set -euo pipefail

echo "🚀 Deploying EPP Router with sim-program-aware plugin..."
echo ""

# Set environment variables for the deployment
export EPP_NAME="inference-gateway"
export NAMESPACE="default"
export DECODE_ROLE="decode"
export MODEL_NAME="Qwen/Qwen2.5-32B-Instruct"
export KV_CACHE_ENABLED="true"
export VLLM_DATA_PARALLEL_SIZE="4"
export VLLM_REPLICA_COUNT_D="4"
export VLLM_SIM_MODE="decode"
export VLLM_EXTRA_ARGS_D=""
export VLLM_IMAGE="llm-d-inference-sim:v0.9.0"
export POOL_NAME="vllm-qwen3-32b"
export TARGET_PORTS="8000"

echo "Configuration:"
echo "  EPP Name: $EPP_NAME"
echo "  Namespace: $NAMESPACE"
echo "  Target Pods: app=vllm-qwen3-32b (4 replicas)"
echo "  Plugin: sim-program-aware-fairness"
echo ""

# Generate the manifests with environment variable substitution
echo "📝 Generating manifests..."
kubectl kustomize . | envsubst > epp-router-final.yaml

echo "✅ Manifests generated: epp-router-final.yaml"
echo ""

# Check if EPP router already exists
if kubectl get deployment $EPP_NAME -n $NAMESPACE &>/dev/null; then
    echo "⚠️  EPP router deployment '$EPP_NAME' already exists"
    read -p "Do you want to delete and redeploy? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🗑️  Deleting existing deployment..."
        kubectl delete deployment $EPP_NAME -n $NAMESPACE
        kubectl delete service $EPP_NAME -n $NAMESPACE 2>/dev/null || true
        kubectl delete configmap epp-config -n $NAMESPACE 2>/dev/null || true
        echo "✅ Cleanup complete"
        echo ""
    else
        echo "❌ Deployment cancelled"
        exit 0
    fi
fi

# Apply the manifests
echo "🚀 Deploying EPP router..."
kubectl apply -f epp-router-final.yaml -n $NAMESPACE

echo ""
echo "⏳ Waiting for EPP router to be ready..."
kubectl wait --for=condition=available --timeout=120s deployment/$EPP_NAME -n $NAMESPACE

echo ""
echo "✅ EPP Router deployed successfully!"
echo ""
echo "📊 Deployment Status:"
kubectl get deployment $EPP_NAME -n $NAMESPACE
echo ""
kubectl get pods -l app=$EPP_NAME -n $NAMESPACE
echo ""

echo "🔍 EPP Router Configuration:"
kubectl get configmap epp-config -n $NAMESPACE -o yaml | grep -A 20 "epp-config.yaml:"
echo ""

echo "🎯 Target vLLM Pods:"
kubectl get pods -l app=vllm-qwen3-32b -n $NAMESPACE
echo ""

echo "✅ Setup Complete!"
echo ""
echo "Next steps:"
echo "  1. Check EPP router logs: kubectl logs -l app=$EPP_NAME -n $NAMESPACE -f"
echo "  2. Test inference: curl http://<epp-service-ip>:8000/v1/completions"
echo "  3. Monitor routing: kubectl logs -l app=$EPP_NAME -n $NAMESPACE | grep 'program-aware'"
```

**Create and make executable**:
```bash
# Create the script
cat > deploy-epp-router.sh <<'EOF'
[paste the script content above]
EOF

# Make executable
chmod +x deploy-epp-router.sh
```

---

### Step 6: Deploy EPP Router

```bash
# Navigate to the directory
cd deploy/environments/dev/epd

# Run the deployment script
./deploy-epp-router.sh
```

**Expected Output**:
```
🚀 Deploying EPP Router with sim-program-aware plugin...

Configuration:
  EPP Name: inference-gateway
  Namespace: default
  Target Pods: app=vllm-qwen3-32b (4 replicas)
  Plugin: sim-program-aware-fairness

📝 Generating manifests...
✅ Manifests generated: epp-router-final.yaml

🚀 Deploying EPP router...
serviceaccount/inference-gateway created
role.rbac.authorization.k8s.io/inference-gateway created
rolebinding.rbac.authorization.k8s.io/inference-gateway-binding created
configmap/epp-config-xxxxx created
service/inference-gateway created
deployment.apps/inference-gateway created

⏳ Waiting for EPP router to be ready...
deployment.apps/inference-gateway condition met

✅ EPP Router deployed successfully!
```

---

### Step 7: Verify Deployment

```bash
# Check pod status
kubectl get pods -l app=inference-gateway

# Expected: 1/1 Running

# Check service account
kubectl get pods -l app=inference-gateway -o jsonpath='{.items[0].spec.serviceAccountName}'

# Expected: inference-gateway

# Check logs for pod discovery
kubectl logs -l app=inference-gateway -c epp --tail=20

# Expected: Should see "Pod already exists" for 4 vllm pods

# Check service
kubectl get svc inference-gateway

# Expected: ClusterIP service with ports 9002, 5557, 9090
```

---

## 🔍 Verification Checklist

- [ ] 4 sim vLLM pods running (`kubectl get pods -l app=vllm-qwen3-32b`)
- [ ] EPP router pod running (`kubectl get pods -l app=inference-gateway`)
- [ ] EPP router using `inference-gateway` service account
- [ ] EPP router logs show pod discovery (4 vllm pods)
- [ ] ConfigMap contains sim-program-aware config
- [ ] Service `inference-gateway` exists with correct ports

---

## 🐛 Troubleshooting

### Issue: EPP Pod CrashLoopBackOff

**Symptom**: Pod keeps restarting

**Check**:
```bash
kubectl logs -l app=inference-gateway -c epp --tail=50
```

**Common Causes**:
1. **Missing POD_NAME env var** → Check `patch-add-pod-env.yaml` is applied
2. **Wrong service account** → Should be `inference-gateway`, not `default`
3. **RBAC permissions** → Check Role and RoleBinding exist

**Fix**:
```bash
# Verify service account
kubectl get pods -l app=inference-gateway -o jsonpath='{.items[0].spec.serviceAccountName}'

# Should output: inference-gateway

# If wrong, update kustomization.yaml line 32
```

### Issue: vllm-render ImagePullBackOff

**Symptom**: Second container fails to pull image

**Cause**: vllm-render container not removed from deployment

**Fix**:
```bash
# Verify patch-remove-render.yaml exists
ls -la patch-remove-render.yaml

# Verify it's in kustomization.yaml patches list
grep "patch-remove-render" kustomization.yaml

# Redeploy
kubectl delete deployment inference-gateway
./deploy-epp-router.sh
```

### Issue: EPP Can't Discover vLLM Pods

**Symptom**: Logs show "pods is forbidden"

**Cause**: Wrong service account or missing RBAC permissions

**Fix**:
```bash
# Check service account
kubectl get sa inference-gateway

# Check role
kubectl get role inference-gateway -o yaml

# Check role binding
kubectl get rolebinding inference-gateway-binding -o yaml

# Verify pod uses correct SA
kubectl get pods -l app=inference-gateway -o jsonpath='{.items[0].spec.serviceAccountName}'
```

### Issue: Plugin Not Loaded

**Symptom**: Logs don't show program-aware-fairness

**Check**:
```bash
# View config
kubectl get configmap epp-config -o yaml

# Should contain program-aware-fairness plugin
```

**Fix**:
```bash
# Verify sim-program-aware-config.yaml exists
cat sim-program-aware-config.yaml

# Verify it's referenced in kustomization.yaml
grep "sim-program-aware-config" kustomization.yaml

# Redeploy
kubectl delete configmap epp-config
./deploy-epp-router.sh
```

---

## 📊 Monitoring

### View EPP Router Logs
```bash
# Real-time logs
kubectl logs -f -l app=inference-gateway -c epp

# Filter for routing decisions
kubectl logs -l app=inference-gateway -c epp | grep -i "pod\|routing\|fairness"

# Last 50 lines
kubectl logs -l app=inference-gateway -c epp --tail=50
```

### View Metrics
```bash
# Port-forward metrics endpoint
kubectl port-forward svc/inference-gateway 9090:9090

# In another terminal
curl http://localhost:9090/metrics | grep -i "fairness\|program\|queue"
```

### Check Pod Discovery
```bash
# See which pods EPP discovered
kubectl logs -l app=inference-gateway -c epp | grep "Pod already exists"

# Should show 4 vllm pods
```

---

## 🔄 Clean Up

### Remove EPP Router
```bash
kubectl delete deployment inference-gateway
kubectl delete service inference-gateway
kubectl delete configmap epp-config
kubectl delete rolebinding inference-gateway-binding
kubectl delete role inference-gateway
kubectl delete serviceaccount inference-gateway
```

### Remove vLLM Pods
```bash
kubectl delete deployment vllm-qwen3-32b
kubectl delete service vllm-qwen3-32b
```

### Complete Cleanup
```bash
# Delete everything in namespace
kubectl delete all -l app=inference-gateway
kubectl delete all -l app=vllm-qwen3-32b
```

---

## 📁 File Structure

After setup, you should have:

```
deploy/environments/dev/epd/
├── kustomization.yaml                  # Main kustomize config (MODIFIED)
├── sim-program-aware-config.yaml       # Plugin configuration (NEW)
├── patch-remove-render.yaml            # Remove vllm-render (NEW)
├── patch-add-pod-env.yaml              # Add env vars (NEW)
├── deploy-epp-router.sh                # Deployment script (NEW)
├── patch-decode.yaml                   # Existing decode patch
├── patch-gateway-sandbox.yaml          # Existing gateway patch
└── scheduler-sandbox.yaml              # Existing scheduler config
```

---

## ✅ Success Criteria

Your setup is successful when:

1. ✅ `kubectl get pods -l app=inference-gateway` shows `1/1 Running`
2. ✅ `kubectl get pods -l app=vllm-qwen3-32b` shows 4 pods `1/1 Running`
3. ✅ EPP logs show: `"Pod already exists"` for 4 vllm pods
4. ✅ EPP logs show: `"program-aware-fairness"` plugin loaded
5. ✅ Service account is `inference-gateway` (not `default`)
6. ✅ No `ImagePullBackOff` or `CrashLoopBackOff` errors

---

## 🎓 Next Steps

After successful setup:

1. **Test Inference** - Send requests through the router
2. **Monitor Fairness** - Watch how requests are distributed
3. **Customize Plugin** - Modify fairness policy
4. **Build Custom Router** - Create your own routing algorithm

See the main README for more details on testing and customization.

---

## 📞 Quick Reference Commands

```bash
# Deploy everything
cd ~/Documents/Pradnya/llm-d-inference-scheduler/deploy/environments/dev/epd
./deploy-epp-router.sh

# Check status
kubectl get pods -l app=inference-gateway
kubectl get pods -l app=vllm-qwen3-32b

# View logs
kubectl logs -f -l app=inference-gateway -c epp

# View config
kubectl get configmap epp-config -o yaml

# Redeploy
kubectl delete deployment inference-gateway
./deploy-epp-router.sh

# Clean up
kubectl delete all -l app=inference-gateway
```

---

**Last Updated**: 2026-06-08
**Tested On**: Rancher Desktop with Kubernetes v1.28