#!/bin/bash


SHARED_FOLDER=$SHARED_FOLDER_PATH/02_input_SAT
OUTPUT_FOLDER=$SHARED_FOLDER_PATH/03_output_SAT

SCRIPT_DIR=$(dirname "$0")

export SCRIPT_DIR
export OMP_NUM_THREADS=24 # number of cpus for parallel processing - maybe change to adapt for galaxy setup

# mkdir -p "$OUTPUT_FOLDER"


tiles=("$SHARED_FOLDER/*.laz")
    num_tiles=${#tiles[@]}

if [ "$num_tiles" -eq 0 ]; then
    echo "No tiles found in $TILE_DIR. Exiting."
    exit 1
fi


bash "$SCRIPT_DIR/run_oracle_pipeline.sh"

# unzip -o $OUTPUT_FOLDER/*.zip -d $OUTPUT_FOLDER/
# rsync -avP $OUTPUT_FOLDER/home/datascience/results/ $OUTPUT_FOLDER/
rm -rf $OUTPUT_FOLDER/home && rm -rf $OUTPUT_FOLDER/*zip

echo "Segmentation complete. Results in $OUTPUT_FOLDER"