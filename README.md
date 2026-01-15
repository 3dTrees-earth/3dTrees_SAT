# 3dTrees_SAT

Deep learning-based **tree instance segmentation** for LiDAR point clouds, packaged for the 3dTrees workflow.

This repository wraps the **SegmentAnyTree** inference pipeline and ships it as a **GPU Docker image** with a small CLI (`src/run.py`) for running inference and collecting outputs consistently.

It is also **optimized for use on Galaxy** (packaged as a Galaxy tool) and can be run directly on **usegalaxy.eu**:

[Click here to run the tool on galaxy](https://usegalaxy.eu/?tool_id=toolshed.g2.bx.psu.edu%2Frepos%2Fbgruening%2F3dtrees_segmentanytree%2F3dtrees_segmentanytree%2F1.1.0%2Bgalaxy0&version=latest)

Upstream project: [SegmentAnyTree](https://github.com/SmartForest-no/SegmentAnyTree)

## Overview

- **Input**: a single `.laz` file, or a `.zip` containing one or more `.laz` files
- **Output**: segmented `.laz` files written to `03_output_SAT/final_results/`
- **Optional**: `resource_usage.log` (CPU/memory/GPU utilization sampled over time)

## Requirements

- **Linux** host recommended
- **NVIDIA GPU** + working NVIDIA drivers
- **Docker** with NVIDIA Container Toolkit (so `docker run --gpus ...` works)

## Run on Galaxy

If you’re using Galaxy, prefer the Galaxy integration (it wraps the same containerized workflow and handles inputs/outputs in a Galaxy-friendly way):

- `https://usegalaxy.eu/?tool_id=toolshed.g2.bx.psu.edu%2Frepos%2Fbgruening%2F3dtrees_segmentanytree%2F3dtrees_segmentanytree%2F1.1.0%2Bgalaxy0&version=latest`

## Build

From the repo root:

```bash
docker build -t 3dtrees_sat .
```

## Run (single file)

```bash
docker run --rm --gpus all \
  -v "/absolute/path/to/input.laz":/in/input.laz:ro \
  -v "/absolute/path/to/out_dir":/out \
  3dtrees_sat \
  python3.8 /src/run.py --dataset-path /in/input.laz --output-dir /out --log-file true
```

### Outputs (single file)

- **Results folder**: `/absolute/path/to/out_dir/03_output_SAT/final_results/`
- **Convenience copy**: if exactly one output `.laz` is produced, it is renamed to `segmented_pc.laz` and copied to the container working directory
- **Resource log (optional)**: `/absolute/path/to/out_dir/resource_usage.log`

## Run (zip input)

If `--dataset-path` points to a `.zip`, it is extracted into `--output-dir` and then processed. On success, `processed_files.zip` is created in the container working directory.

```bash
docker run --rm --gpus all \
  -v "/absolute/path/to/inputs.zip":/in/inputs.zip:ro \
  -v "/absolute/path/to/out_dir":/out \
  3dtrees_sat \
  python3.8 /src/run.py --dataset-path /in/inputs.zip --output-dir /out --log-file true
```

## CLI reference

Inside the container:

```bash
python3.8 /src/run.py \
  --dataset-path <path/to/input.laz|input.zip> \
  --output-dir <output_directory> \
  --log-file <true|false>
```

## Output folder structure

`--output-dir` becomes the shared workspace for the pipeline and will contain:

- `00_original/` (created)
- `01_subsampled/` (created)
- `02_input_SAT/` (created; inputs are copied here)
- `03_output_SAT/final_results/` (segmented `.laz` results)

## CI / publishing

On GitHub Release publish, `.github/workflows/main.yml` builds and pushes the image to GHCR under:

- `ghcr.io/3dtrees-earth/<repo_name>:<version>`
- `ghcr.io/3dtrees-earth/<repo_name>:latest` (default branch only)
