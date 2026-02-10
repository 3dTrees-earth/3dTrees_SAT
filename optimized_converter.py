import numpy as np
import laspy
from plyfile import PlyElement, PlyData
import json
import os
import time

def optimized_las_to_ply(input_path, output_ply_path, output_json_path):
    """
    Optimized conversion from LAS/LAZ to PLY.
    Skips Pandas and Python-level loops for maximum performance.
    """
    start_time = time.time()
    print(f"Reading {input_path}...")
    
    # 1. Read LAS/LAZ file
    # laspy.read() is faster than laspy.open() for full file access
    las = laspy.read(input_path)
    
    # 2. Identify dimensions to include
    # We want all standard dimensions plus extra ones (like PredInstance)
    basic_dims = [dim.name for dim in las.header.point_format.dimensions]
    extra_dims = list(las.header.point_format.extra_dimension_names)
    all_dims = basic_dims + extra_dims
    
    # 3. Calculate min values for normalization (UTM to Local)
    min_x, min_y, min_z = las.x.min(), las.y.min(), las.z.min()
    
    # 4. Create structured array for PLY
    # We use 'f4' (float32) for coordinates and appropriate types for others
    # Note: SAT model usually expects float32 coordinates
    dtype = []
    for dim in all_dims:
        name = dim.replace(" ", "_")
        # Map specific dimensions to types if needed, otherwise default to f4
        if name in ['X', 'Y', 'Z', 'x', 'y', 'z']:
            dtype.append((name.lower(), 'f4'))
        else:
            dtype.append((name, 'f4')) # Standardizing to f4 for simplicity/compatibility
    
    data = np.empty(las.header.point_count, dtype=dtype)
    
    # 5. Fill data and normalize coordinates
    for dim in all_dims:
        name = dim.replace(" ", "_")
        target_name = name.lower() if name.lower() in ['x', 'y', 'z'] else name
        
        val = getattr(las, dim)
        
        if target_name == 'x':
            data[target_name] = (val - min_x).astype('f4')
        elif target_name == 'y':
            data[target_name] = (val - min_y).astype('f4')
        elif target_name == 'z':
            data[target_name] = (val - min_z).astype('f4')
        else:
            data[target_name] = val.astype('f4')

    # 6. Save min values to JSON (for local2utm reconstruction later)
    with open(output_json_path, 'w') as jf:
        print(f"Saving min values to: {output_json_path}")
        json.dump([float(min_x), float(min_y), float(min_z)], jf)
    
    # 7. Write PLY (Binary format)
    print(f"Writing {output_ply_path}...")
    el = PlyElement.describe(data, 'vertex')
    PlyData([el], text=False).write(output_ply_path)
    
    end_time = time.time()
    print(f"Finished conversion in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 4:
        optimized_las_to_ply(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        print("Usage: python optimized_converter.py <input.laz> <output.ply> <output_min.json>")
