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
export VLLM_RENDER_IMAGE="vllm/vllm-openai-cpu:v0.19.1"
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

# Made with Bob
