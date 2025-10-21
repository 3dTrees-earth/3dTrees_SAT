#!/bin/bash
LOG_FILE="$1"
INTERVAL="${2:-1}"

echo "timestamp,cpu_percent,cpu_cores_used,cpu_cores_total,mem_used_mb,mem_total_mb,gpu_mem_used_mb,gpu_mem_total_mb" >> "$LOG_FILE"

while true; do
    timestamp=$(date +"%Y-%m-%d %H:%M:%S")
    
    # CPU percentage (system-wide)
    cpu_percent=$(top -bn1 | grep "Cpu(s)" | awk '{print $2 + $4}')
    
    # CPU cores tracking
    cpu_cores_total=$(nproc)
    # Count active CPU cores (cores with utilization > 0)
    cpu_cores_used=$(top -bn1 | grep "Cpu(s)" | awk -v cores="$cpu_cores_total" '{cpu_util=$2+$4; printf "%d", (cpu_util/100)*cores}')
    
    # Memory (system-wide)
    mem_used=$(free -m | awk '/Mem:/ {print $3}')
    mem_total=$(free -m | awk '/Mem:/ {print $2}')
    
    # GPU RAM (NVIDIA only)
    if command -v nvidia-smi &> /dev/null; then
        gpu_mem=$(nvidia-smi --query-gpu=memory.used,memory.total --format=csv,noheader,nounits | head -n1)
        gpu_mem_used=$(echo $gpu_mem | cut -d',' -f1 | xargs)
        gpu_mem_total=$(echo $gpu_mem | cut -d',' -f2 | xargs)
    else
        gpu_mem_used=NA
        gpu_mem_total=NA
    fi
    
    echo "$timestamp,$cpu_percent,$cpu_cores_used,$cpu_cores_total,$mem_used,$mem_total,$gpu_mem_used,$gpu_mem_total" >> "$LOG_FILE"
    sleep "$INTERVAL"
done