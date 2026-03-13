import laspy
import pandas as pd

# works with laspy 2.1.2 (the other versions are not tested)


def pandas_to_las(
    csv,
    csv_file_provided=False,
    output_file_path=None,
    do_compress=False,
    verbose=False,
):
    """
    Convert a pandas DataFrame to a .las file.

    Parameters
    ----------
    csv : pandas DataFrame
        The DataFrame to be converted to .las file.
        But if the csv_file_provided argument is true,
        the csv argument is considered as the path to the .csv file.
    las_file_path : str
        The path to the .las file to be created.
    csv_file_provided : str, optional
        The path to the .csv file to be converted to .las file.
        If None, the csv argument is used instead.
        The default is None.
    """
    # Check if the csv_file_provided argument is provided

    if csv_file_provided:
        df = pd.read_csv(csv, sep=",")
    else:
        df = csv

    standard_columns_with_data_types = {
        "X": "int32",
        "Y": "int32",
        "Z": "int32",
        "intensity": "uint16",
        "return_number": "uint8",
        "number_of_returns": "uint8",
        "synthetic": "uint8",
        "key_point": "uint8",
        "withheld": "uint8",
        "overlap": "uint8",  # Data type not specified in the provided formats
        "scanner_channel": "uint8",  # Data type not specified in the provided formats
        "scan_direction_flag": "uint8",
        "edge_of_flight_line": "uint8",
        "classification": "uint8",
        "user_data": "uint8",
        "scan_angle": "uint16",  # Data type not specified in the provided formats
        # 'scan_angle_rank': 'int8',
        "point_source_id": "uint16",
        "gps_time": "float64",
        "red": "uint16",  # Data type not specified in the provided formats
        "green": "uint16",  # Data type not specified in the provided formats
        "blue": "uint16",  # Data type not specified in the provided formats
    }

    # Drop training-only ground-truth columns that should not be written
    # back to the LAS file. The SAT tool should only add PredSemantic
    # and PredInstance on top of the original input dimensions.
    gt_prefix = "gt_semantic_segmentation"
    gt_cols_to_drop = [
        col for col in df.columns
        if col == gt_prefix or col.startswith(gt_prefix + "_")
    ]
    if gt_cols_to_drop:
        df = df.drop(columns=gt_cols_to_drop)

    extended_columns_with_data_types = {
        "Amplitude": "float64",
        "Pulse_width": "float64",
        "Reflectance": "float64",
        "Deviation": "int32",
        # Prediction outputs we always want to have in the LAS file
        # in addition to any input dimensions.
        "PredSemantic": "uint8",
        "PredInstance": "uint16",
    }

    # Dynamically add data types for any _original, _current, or _new columns found in the DataFrame
    for col in df.columns:
        if (col.endswith("_original") or col.endswith("_current") or col.endswith("_new")) and col not in extended_columns_with_data_types:
            # Determine data type based on the base column name
            base_name = col.replace("_original", "").replace("_current", "").replace("_new", "")
            
            if base_name in ["PredSemantic", "gt_semantic_segmentation"]:
                extended_columns_with_data_types[col] = "uint8"
            elif base_name in ["PredInstance", "PredInstanceSAT"]:
                extended_columns_with_data_types[col] = "uint16"
            elif base_name in ["red", "green", "blue"]:
                extended_columns_with_data_types[col] = "uint16"
            elif base_name in standard_columns_with_data_types:
                # Use the same type as the base column
                extended_columns_with_data_types[col] = standard_columns_with_data_types[base_name]
            else:
                # Default to float64 for unknown columns
                extended_columns_with_data_types[col] = "float64"
    
    # Also handle numbered suffixes (e.g., red_1, green_1, etc.)
    for col in df.columns:
        if col not in standard_columns_with_data_types and col not in extended_columns_with_data_types:
            # Check if it's a numbered variant (e.g., red_1, green_1)
            if '_' in col and col.split('_')[-1].isdigit():
                base_name = '_'.join(col.split('_')[:-1])
                if base_name in standard_columns_with_data_types:
                    extended_columns_with_data_types[col] = standard_columns_with_data_types[base_name]
                elif base_name in ["PredSemantic", "gt_semantic_segmentation"]:
                    extended_columns_with_data_types[col] = "uint8"
                elif base_name in ["PredInstance", "PredInstanceSAT"]:
                    extended_columns_with_data_types[col] = "uint16"
                else:
                    extended_columns_with_data_types[col] = "float64"

    # Standardize column names to match LAS format
    df.rename(columns={"x": "X", "y": "Y", "z": "Z"}, inplace=True)

    # Calculate scales and offsets for your point data
    scale = [0.001, 0.001, 0.001]  # Example scale factors
    offset = [df["X"].min(), df["Y"].min(), df["Z"].min()]  # Minimum values as offsets

    # Create a new .las file with correct header information
    las_header = laspy.LasHeader(point_format=6, version="1.4")
    las_header.scale = scale
    las_header.offset = offset

    # Bounds
    min_bounds = offset  # already calculated as minimums
    max_bounds = [df["X"].max(), df["Y"].max(), df["Z"].max()]  # Maximum values
    las_header.min = min_bounds
    las_header.max = max_bounds

    # if there is scan_angle_rank column map it to scan_angle
    if "scan_angle_rank" in df.columns:
        df.rename(columns={"scan_angle_rank": "scan_angle"}, inplace=True)

    # check if the columns in the dataframe match the standard columns and make a list of the columns that match
    standard_columns = list(las_header.point_format.dimension_names)
    columns_which_match = [
        column for column in standard_columns if column in df.columns
    ]

    # remove X, Y and Z from the list
    columns_which_match.remove("X")
    columns_which_match.remove("Y")
    columns_which_match.remove("Z")

    # get extra columns as columns which don't match
    extra_columns = [column for column in df.columns if column not in standard_columns]

    # check if the extra columns exist in the extended_columns_with_data_types dictionary
    # for those which do not exist, take the data type from data frame
    for column in extra_columns:
        if column not in extended_columns_with_data_types.keys():
            # Handle special cases for prediction columns
            if column.startswith("PredInstance"):
                extended_columns_with_data_types[column] = "uint16"
            elif column.startswith("PredSemantic"):
                extended_columns_with_data_types[column] = "uint8"
            else:
                extended_columns_with_data_types[column] = df[column].dtype

    # add extra columns to the las file with the correct data types
    # LAS format has a 32-character limit for dimension names, so we need to shorten long names
    column_name_mapping = {}
    
    for column in extra_columns:
        las_column_name = column
        
        # Shorten names that are too long (>32 characters)
        if len(column) > 32:
            if column == "gt_semantic_segmentation_original":
                las_column_name = "gt_semantic_seg_original"  # 23 chars
            elif column.startswith("gt_semantic_segmentation_original_"):
                # Handle numbered variants like gt_semantic_segmentation_original_1
                suffix = column.replace("gt_semantic_segmentation_original_", "")
                las_column_name = f"gt_semantic_seg_orig_{suffix}"  # Much shorter
            else:
                # Generic shortening for other long names
                las_column_name = column[:32]
        
        column_name_mapping[column] = las_column_name
        
        las_header.add_extra_dim(
            laspy.ExtraBytesParams(
                name=las_column_name, type=extended_columns_with_data_types[column]
            )
        )

    # create a new las file with the correct header information
    las_file = laspy.LasData(las_header)

    # Assigning the scaled and offset data
    las_file.X = (df["X"] - offset[0]) / scale[0]
    las_file.Y = (df["Y"] - offset[1]) / scale[1]
    las_file.Z = (df["Z"] - offset[2]) / scale[2]

    # add standard columns to the las file with the correct data types
    for column in columns_which_match:
        las_file[column] = df[column].astype(standard_columns_with_data_types[column])

    # add extra columns to the las file with the correct data types
    for column in extra_columns:
        las_column_name = column_name_mapping.get(column, column)
        las_file[las_column_name] = df[column].astype(extended_columns_with_data_types[column])

    # Write the file to disk
    if do_compress:
        output_file_path = output_file_path.replace(".las", ".laz")
        las_file.write(output_file_path, do_compress=True)
    else:
        las_file.write(output_file_path, do_compress=False)

    if verbose:
        if csv_file_provided:
            print("The input file was is {}".format(csv))

        if do_compress:
            print("File saved as: {}".format(output_file_path.replace(".las", ".laz")))
        else:
            print("File saved as: {}".format(output_file_path))
