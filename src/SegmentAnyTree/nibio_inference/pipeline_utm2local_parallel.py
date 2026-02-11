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
        # Handle potential duplicate field names (e.g., PredInstance appearing multiple times)
        las = None
        read_error = None
        
        # Try reading with laspy.read() first
        try:
            las = laspy.read(input_file_path)
        except Exception as e:
            error_str = str(e).lower()
            if "occurs more than once" in str(e) or "duplicate" in error_str or "field" in error_str:
                # If duplicate field error, try alternative reading methods
                print(f"  ⚠️  Warning: Duplicate field detected in LAS file, attempting alternative reading method...")
                read_error = e
                
                # Try using laspy.open() - sometimes this handles files differently
                try:
                    with laspy.open(input_file_path) as las_file:
                        las = las_file.read()
                        print(f"  ✓ Successfully read file using laspy.open()")
                except Exception as e2:
                    # If that also fails, try reading with lazrs backend if available
                    try:
                        with laspy.open(input_file_path, laz_backend=laspy.LazBackend.LazrsParallel) as las_file:
                            las = las_file.read()
                            print(f"  ✓ Successfully read file using lazrs backend")
                    except Exception as e3:
                        # Last resort: provide helpful error message
                        error_msg = (
                            f"❌ ERROR: Could not read LAS file '{os.path.basename(input_file_path)}' due to duplicate field names.\n"
                            f"   The file appears to have duplicate 'PredInstance' field definitions.\n"
                            f"   This is a file structure issue that prevents laspy from reading the file.\n"
                            f"   Original error: {read_error}\n"
                            f"   Suggestion: The file may need to be repaired or the duplicate fields removed/renamed."
                        )
                        print(f"  {error_msg}")
                        raise Exception(error_msg) from read_error
            else:
                raise
        
        # 2. Get all dimensions, handling duplicates
        basic_dims = [dim.name for dim in las.header.point_format.dimensions]
        extra_dims = list(las.header.point_format.extra_dimension_names)
        
        # Deduplicate the dimension list to avoid processing the same dimension twice
        all_input_dims = []
        seen_input_dims = set()
        for dim in basic_dims + extra_dims:
            if dim not in seen_input_dims:
                all_input_dims.append(dim)
                seen_input_dims.add(dim)
        
        # Check for duplicate dimension names (case-insensitive)
        seen_dims = set()
        all_dims = []
        dim_renames = {}  # Track renames for PredInstance -> PredInstance_original, etc.
        
        for dim in all_input_dims:
            dim_lower = dim.lower()
            
            # Always rename PredInstance and PredSemantic to preserve input data
            if dim_lower == "predinstance":
                new_name = "PredInstance_original"
                dim_renames[dim] = new_name
                all_dims.append(new_name)
                seen_dims.add(new_name.lower())
                print(f"  ⚠️  Warning: Renaming '{dim}' to '{new_name}' to preserve input data")
            elif dim_lower == "predsemantic":
                new_name = "PredSemantic_original"
                dim_renames[dim] = new_name
                all_dims.append(new_name)
                seen_dims.add(new_name.lower())
                print(f"  ⚠️  Warning: Renaming '{dim}' to '{new_name}' to preserve input data")
            elif dim_lower in seen_dims:
                # This is a duplicate of other dimensions - handle it
                base_name = f"{dim}_original"
                new_name = base_name
                counter = 1
                while new_name.lower() in seen_dims or new_name in all_dims:
                    new_name = f"{base_name}_{counter}"
                    counter += 1
                
                dim_renames[dim] = new_name
                all_dims.append(new_name)
                seen_dims.add(new_name.lower())
                print(f"  ⚠️  Warning: Renaming duplicate dimension '{dim}' to '{new_name}'")
            else:
                # This is the first occurrence - keep it as is
                all_dims.append(dim)
                seen_dims.add(dim_lower)
        
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
            # Check if this dimension was renamed
            original_dim = None
            for orig, renamed in dim_renames.items():
                if renamed == dim:
                    original_dim = orig
                    break
            
            attr_name = original_dim if original_dim else (dim.lower() if dim.lower() in ['x', 'y', 'z'] else dim)
            
            try:
                val = np.array(getattr(las, attr_name))
            except AttributeError:
                # If attribute doesn't exist, try with different case or skip
                try:
                    val = np.array(getattr(las, dim))
                except AttributeError:
                    print(f"  ⚠️  Warning: Could not access dimension '{dim}', skipping...")
                    continue
            
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
