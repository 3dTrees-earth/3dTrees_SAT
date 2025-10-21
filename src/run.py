#!/usr/bin/env python3.8
import os
import subprocess
import sys
import zipfile
import time
from datetime import datetime

try:
    from parameters import Parameters
except Exception as e:
    print(f"Error importing Parameters: {e}")
    sys.exit(1)


def extract_zip_to_output(zip_path, output_dir):
    """Extract the input zip file to the output directory."""
    print(f"Extracting {zip_path} to {output_dir}")
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(output_dir)
    print("Extraction complete.")


def process_input_files(params: Parameters, output_dir: str) -> bool:
    """Process input files in the 02_input_SAT directory."""
    data_dir = os.path.abspath(output_dir)

    # Ensure output SAT directory exists
    os.makedirs(f"{data_dir}/03_output_SAT", exist_ok=True)

    # Check if 02_input_SAT directory exists
    input_sat_dir = f"{data_dir}/02_input_SAT"
    if not os.path.exists(input_sat_dir):
        print(f"Error: Directory {input_sat_dir} not found!")
        return False

    # Set environment variables for the segmentation script
    env = os.environ.copy()
    env.update(
        {
            "SHARED_FOLDER_PATH": data_dir,
            "ENABLE_LOG_FILE": params.log_file,
        }
    )

    # Run only the segmentation part
    print("Running segmentation...")
    subprocess.run(["bash", "/src/main.sh"], cwd="/", env=env)

    print(f"Segmentation complete. Results are in {data_dir}/03_output_SAT")
    return True


def create_results_zip(output_dir: str, params: Parameters) -> bool:
    """Create a zip file of the final folder structure in the current working directory."""
    data_dir = os.path.abspath(output_dir)

    # Check if required directories exist
    required_dirs = ["00_original", "01_subsampled", "02_input_SAT", "03_output_SAT"]
    for dir_name in required_dirs:
        if not os.path.exists(f"{data_dir}/{dir_name}"):
            print(f"Warning: Directory {dir_name} not found in output directory.")

    # Create zip file in the current working directory
    print("Creating zip file of the final folder structure...")
    zip_file_path = "processed_files.zip"

    try:
        with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Add each directory to the zip file
            for dir_name in required_dirs:
                dir_path = os.path.join(data_dir, dir_name)
                if os.path.exists(dir_path):
                    # Walk through the directory and add all files
                    for root, _, files in os.walk(dir_path):
                        for file in files:
                            file_path = os.path.join(root, file)
                            # Add file with path relative to the output_dir
                            arcname = os.path.relpath(file_path, data_dir)
                            zipf.write(file_path, arcname)

            # If log-file is enabled, add the resource log file
            if params.log_file.lower() == "true":
                log_file_path = os.path.join(data_dir, "resource_usage.log")
                if os.path.exists(log_file_path):
                    zipf.write(log_file_path, "resource_usage.log")
                    print("Added resource usage log to zip file")

        print(f"Zip file created: {os.path.abspath(zip_file_path)}")
        return True
    except Exception as e:
        print(f"Error creating zip file: {e}")
        return False


def main():
    # Parse parameters from CLI using Parameters class
    params = Parameters()

    # Start timing
    start_time = time.time()
    start_datetime = datetime.now()
    print(
        f"=== Processing started at {start_datetime.strftime('%Y-%m-%d %H:%M:%S')} ==="
    )

    if not os.path.isfile(params.dataset_path):
        print(f"Input file not found: {params.dataset_path}")
        sys.exit(2)

    if not params.dataset_path.lower().endswith(".zip"):
        print(f"Input file must be a ZIP file: {params.dataset_path}")
        sys.exit(3)

    # Create output directory if it doesn't exist
    os.makedirs(params.output_dir, exist_ok=True)

    # Extract zip file to output directory
    extract_zip_to_output(params.dataset_path, params.output_dir)

    # Process the extracted files
    success = process_input_files(params, params.output_dir)

    if success:
        # Create a zip file of the final folder structure
        create_results_zip(params.output_dir, params)

    # End timing
    end_time = time.time()
    end_datetime = datetime.now()
    duration = end_time - start_time

    print(
        f"\n=== Processing completed at {end_datetime.strftime('%Y-%m-%d %H:%M:%S')} ==="
    )
    print(
        f"Total processing time: {duration:.2f} seconds ({duration / 60:.2f} minutes)"
    )
    print("Processing complete!")


if __name__ == "__main__":
    main()
