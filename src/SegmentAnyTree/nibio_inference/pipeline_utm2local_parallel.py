import argparse
import json
import os
import numpy as np
import laspy
from plyfile import PlyElement, PlyData
from joblib import Parallel, delayed
import time

def process_file(filename, input_folder, output_folder):
    input_file_path = os.path.join(input_folder, filename)
    base_filename = os.path.splitext(filename)[0]
    output_file_path = os.path.join(output_folder, f"{base_filename}_out.ply")
    json_file_path = os.path.join(output_folder, f"{base_filename}_out_min_values.json")

    if filename.endswith((".las", ".laz")):
        optimized_modification_pipeline(input_file_path, output_file_path, json_file_path)
    elif filename.endswith(".ply"):
        # Fallback for PLY files if needed, though less common in this pipeline
        print(f"Skipping PLY file in optimized pipeline: {filename}")

def optimized_modification_pipeline(input_file_path, output_file_path, json_file_path):
    """
    Optimized UTM to Local conversion.
    Directly reads LAS/LAZ and writes normalized PLY using numpy structured arrays.
    """
    print(f"Processing in utm2local (OPTIMIZED): {input_file_path}")
    start_time = time.time()
    
    try:
        # 1. Read LAS/LAZ
        las = laspy.read(input_file_path)
        
        # 2. Get all dimensions
        basic_dims = [dim.name for dim in las.header.point_format.dimensions]
        extra_dims = list(las.header.point_format.extra_dimension_names)
        all_dims = basic_dims + extra_dims
        
        # 3. Calculate min values - convert to Python floats to avoid SubFieldView issues
        min_x = float(np.array(las.x).min())
        min_y = float(np.array(las.y).min())
        min_z = float(np.array(las.z).min())
        
        # 4. Prepare structured array for PLY
        dtype = []
        for dim in all_dims:
            name = dim.replace(" ", "_")
            if name.lower() in ['x', 'y', 'z']:
                dtype.append((name.lower(), 'f4'))
            else:
                dtype.append((name, 'f4'))
        
        data = np.empty(las.header.point_count, dtype=dtype)
        
        # 5. Fill and normalize
        for dim in all_dims:
            name = dim.replace(" ", "_")
            target_name = name.lower() if name.lower() in ['x', 'y', 'z'] else name
            
            # Wrap in np.array to handle SubFieldView objects from laspy
            # IMPORTANT: Use lowercase for coordinate access in laspy (las.x, las.y, las.z)
            attr_name = dim.lower() if dim.lower() in ['x', 'y', 'z'] else dim
            val = np.array(getattr(las, attr_name))
            
            if target_name == 'x':
                data[target_name] = (val - min_x).astype('f4')
            elif target_name == 'y':
                data[target_name] = (val - min_y).astype('f4')
            elif target_name == 'z':
                data[target_name] = (val - min_z).astype('f4')
            else:
                data[target_name] = val.astype('f4')

        # 6. Save min values
        with open(json_file_path, 'w') as f:
            json.dump([float(min_x), float(min_y), float(min_z)], f)
            
        # 7. Write PLY
        el = PlyElement.describe(data, 'vertex')
        PlyData([el], text=False).write(output_file_path)
        
        print(f"  ✓ Finished {os.path.basename(input_file_path)} in {time.time() - start_time:.2f}s")
        
    except Exception as e:
        print(f"  ❌ Error processing {input_file_path}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process las or laz files and save results as ply files (OPTIMIZED)."
    )
    parser.add_argument(
        "-i",
        "--input_folder",
        type=str,
        help="Path to the input folder containing LAS/LAZ files.",
    )
    parser.add_argument(
        "-o",
        "--output_folder",
        type=str,
        help="Path to the output folder to save PLY files.",
    )

    args = parser.parse_args()

    os.makedirs(args.output_folder, exist_ok=True)
    filenames = [f for f in os.listdir(args.input_folder) if f.endswith((".las", ".laz"))]

    print(f"Processing {len(filenames)} files with optimized pipeline...")
    # Using fewer jobs for large files to avoid memory pressure
    Parallel(n_jobs=2)(
        delayed(process_file)(filename, args.input_folder, args.output_folder)
        for filename in filenames
    )
    print(f"Output files are saved in: {args.output_folder}")
