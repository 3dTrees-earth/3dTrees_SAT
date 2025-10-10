# 🌳 Til_SAT: Segment Any Tree - optimized for 3dTrees

**Deep learning-based tree segmentation from LiDAR point clouds**  
Powered by the **SegmentAnyTree** algorithm ([Wielgosz et al., 2024](https://doi.org/10.1016/j.rse.2024.114367))  

---

## 📌 Overview  

**Til_SAT** performs **deep learning-based tree segmentation** on LiDAR point clouds.  
It uses a sensor- and platform-agnostic model that works across **airborne (ALS/ULS), terrestrial (TLS), and mobile (MLS) laser scanning data**.  

The tool automatically handles **subsampling, tiling, segmentation, and merging** to produce a **segmented point cloud at the original resolution**.  

---

## ⚙️ Features  

- 🌐 **Platform agnostic** – works with ALS, TLS, ULS, MLS datasets  
- 📏 **Voxel-based subsampling** at 100 pts/m²  
- 🧩 **Automatic tiling** for large point clouds (>3 GB)  
- 🤖 **Deep learning segmentation** using SegmentAnyTree  
- 🔄 **Merging** results back into the original resolution point cloud  
- 📂 **Output**: Segmented `.laz` point cloud  

---

## 📥 Input  

- **LiDAR point cloud**: `.laz` or `.las` format  

---

## 📤 Output  

- **Segmented Point Cloud**: `.laz` file with tree segmentation results

---

## 🚀 Usage  

```bash
python3.8 -u /src/run.py \
    --dataset-path <input.laz> \
    --output-dir /out/ \
    --tile-size 100 \
    --overlap 20 
```

## Workflow

### 1. Data Preparation
- Voxel-based subsampling to 10 cm resolution (100 pts/m²)
- Automatic tiling for large datasets (>3 GB)

### 2. Tree Segmentation
- Deep learning inference using SegmentAnyTree model
- Instance segmentation to identify individual trees

### 3. Quality Control
- Whole tree filtering - trees extending into buffer zones are excluded
- Configurable buffer zones for optimal results

### 4. Resolution Recovery
- KDTree-based reassignment of subsampled results to original resolution
- Seamless merging of tile results back into full point cloud

### 5. Output Generation
- Segmented point cloud (.laz format) with tree instance IDs
- Quality metrics and processing statistics
