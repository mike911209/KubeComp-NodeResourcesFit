#!/bin/bash

NODE_NAME="minikube-m02"
DEVICE_NAME="nvidia.com~1mig-1g.5gb"

curl --header "Content-Type: application/json-patch+json" \
  --request PATCH \
  --data '[{"op": "add", "path": "/status/capacity/nvidia.com~1mig-1g.5gb", "value": "7"}]' \
  http://localhost:8001/api/v1/nodes/$NODE_NAME/status