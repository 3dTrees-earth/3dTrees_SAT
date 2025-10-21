#!/bin/bash

# Ensure all required directories exist
mkdir -p "${SHARED_FOLDER_PATH}/00_original" \
         "${SHARED_FOLDER_PATH}/01_subsampled" \
         "${SHARED_FOLDER_PATH}/02_input_SAT" \
         "${SHARED_FOLDER_PATH}/03_output_SAT"

echo "Starting segmentation process..."
echo "Input directory: ${SHARED_FOLDER_PATH}/02_input_SAT"
echo "Output directory: ${SHARED_FOLDER_PATH}/03_output_SAT"

# Check if input directory has files
INPUT_COUNT=$(ls -1q "${SHARED_FOLDER_PATH}/02_input_SAT"/*.laz 2>/dev/null | wc -l)
if [ "$INPUT_COUNT" -eq 0 ]; then
    echo "ERROR: No .laz files found in ${SHARED_FOLDER_PATH}/02_input_SAT"
    echo "Contents of directory:"
    ls -la "${SHARED_FOLDER_PATH}/02_input_SAT/" || echo "Cannot list input directory"
    exit 1
fi

echo "Found $INPUT_COUNT .laz files to process"

####################### SEGMENTATION ########################

# Create required directories for segmentation if they don't exist
mkdir -p "/tmp/bucket_in_folder" \
         "/tmp/bucket_out_folder"

# Copy input files to the processing directory
cp "${SHARED_FOLDER_PATH}/02_input_SAT"/*.laz "/tmp/bucket_in_folder/"

# Start resource monitoring in background if enabled
RESOURCE_MONITOR_PID=""
if [ "${ENABLE_LOG_FILE}" = "true" ]; then
    echo "Starting resource monitoring (CPU cores, memory, GPU, etc.)..."
    LOG_FILE="${SHARED_FOLDER_PATH}/resource_usage.log"
    bash /src/resource_monitor.sh "$LOG_FILE" 5 &
    RESOURCE_MONITOR_PID=$!
fi

# Function to cleanup resource monitoring
cleanup_monitor() {
    if [ -n "$RESOURCE_MONITOR_PID" ]; then
        echo "Stopping resource monitoring..."
        kill $RESOURCE_MONITOR_PID 2>/dev/null
        wait $RESOURCE_MONITOR_PID 2>/dev/null
    fi
}

# Set trap to cleanup on exit
trap cleanup_monitor EXIT

# Run segmentation
echo "Running SegmentAnyTree..."
# No conda activation needed for segmentation-only workflow
bash /src/SegmentAnyTree/run_SAT.sh

# Extract and move results to output folder
echo "Processing results..."
if [ -f "/tmp/bucket_out_folder/results.zip" ]; then
    unzip "/tmp/bucket_out_folder/results.zip" -d "/tmp/bucket_out_folder"
    rm -f "/tmp/bucket_out_folder/results.zip"
    
    # Create final results directory
    mkdir -p "${SHARED_FOLDER_PATH}/03_output_SAT/final_results"
    
    # Move results to output folder
    mv "/tmp/bucket_out_folder/home/datascience/results"/* "${SHARED_FOLDER_PATH}/03_output_SAT/final_results/"
    
    # Clean up
    rm -rf "/tmp/bucket_out_folder/home"
    rm -f "/tmp/bucket_in_folder"/*.laz
else
    echo "WARNING: No results.zip file found in /tmp/bucket_out_folder"
    echo "Contents of directory:"
    ls -la "/tmp/bucket_out_folder/" || echo "Cannot list output directory"
fi

echo "Segmentation process completed."