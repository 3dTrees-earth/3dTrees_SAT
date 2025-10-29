#!/usr/bin/env python3.8
import os
import subprocess
import sys
import zipfile
import time
import shutil
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


def handle_single_laz_file(laz_path, output_dir):
    """Handle a single LAZ file by copying it to the input SAT directory."""
    print(f"Processing single LAZ file: {laz_path}")

    # Ensure output SAT directory exists
    input_sat_dir = os.path.join(output_dir, "02_input_SAT")
    os.makedirs(input_sat_dir, exist_ok=True)

    # Copy the LAZ file to the input SAT directory
    dest_path = os.path.join(input_sat_dir, os.path.basename(laz_path))
    shutil.copy2(laz_path, dest_path)
    print(f"Copied {laz_path} to {dest_path}")

    return input_sat_dir


def process_input_files(
    params: Parameters, output_dir: str, is_single_file: bool = False
) -> bool:
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

    # If processing a single file, rename the output to "segmented_pc.laz"
    if is_single_file:
        rename_output_to_segmented_pc(data_dir)

    return True


def rename_output_to_segmented_pc(data_dir: str):
    """Rename the output file to 'segmented_pc.laz' after processing a single file and copy to cwd."""
    output_sat_dir = os.path.join(data_dir, "03_output_SAT")
    final_results_dir = os.path.join(output_sat_dir, "final_results")

    # Look for processed LAZ files in the final results directory
    if os.path.exists(final_results_dir):
        laz_files = [f for f in os.listdir(final_results_dir) if f.endswith(".laz")]

        if len(laz_files) == 1:
            original_file = os.path.join(final_results_dir, laz_files[0])
            new_file = os.path.join(final_results_dir, "segmented_pc.laz")
            os.rename(original_file, new_file)
            print(f"Renamed output file to: {new_file}")

            # Copy to current working directory (same behavior as zip output)
            cwd_file = os.path.join(os.getcwd(), "segmented_pc.laz")
            shutil.copy2(new_file, cwd_file)
            print(f"Copied to current working directory: {os.path.abspath(cwd_file)}")
        elif len(laz_files) > 1:
            print(
                "Warning: Multiple LAZ files found. Please manually rename the desired file to 'segmented_pc.laz'"
            )
        else:
            print("Warning: No LAZ files found in final results directory")


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

    # Determine if input is a ZIP file or a single LAZ file
    is_single_laz = params.dataset_path.lower().endswith(".laz")
    is_zip = params.dataset_path.lower().endswith(".zip")

    if not (is_single_laz or is_zip):
        print(
            f"Input file must be either a ZIP file or a LAZ file: {params.dataset_path}"
        )
        sys.exit(3)

    # Create output directory if it doesn't exist
    os.makedirs(params.output_dir, exist_ok=True)

    # Handle input file based on type
    if is_zip:
        # Extract zip file to output directory
        extract_zip_to_output(params.dataset_path, params.output_dir)
    elif is_single_laz:
        # Copy single LAZ file to input SAT directory
        handle_single_laz_file(params.dataset_path, params.output_dir)

    # Process the extracted/copied files
    success = process_input_files(
        params, params.output_dir, is_single_file=is_single_laz
    )

    if success:
        # Create a zip file of the final folder structure (only for ZIP input)
        if is_zip:
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
