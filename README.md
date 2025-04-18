# Social Media Analytics MapReduce Workflow

This repository hosts a complete MapReduce-based workflow tailored for social media data analytics. It processes two datasets—`social_media_logs.txt` and `user_profiles.txt`—to uncover user behavior patterns, detect trending content, and generate comprehensive analytics by joining both datasets.

---

## 🧩 System Overview

The system is structured into the following five key components:

1. **Data Cleansing and Parsing**  
   Validates and extracts structured fields from raw input data.

2. **Action Aggregation and Sorting**  
   Aggregates user activity and ranks users by post count.

3. **Trending Content Identification**  
   Detects high-engagement content using statistical thresholds.

4. **Dataset Joining**  
   Merges user activity logs with corresponding profile metadata.

5. **Data Visualization**  
   Offers both static and interactive insights from processed data.

---

## 🏗️ System Architecture

![Social Media Analytics Architecture](system_design.png)

The architecture follows a modular pipeline:
- Input is passed through a series of MapReduce jobs
- Intermediate outputs flow into subsequent components
- Final outputs drive visualization and analysis

---

## 💻 Prerequisites

For running on Windows with PyCharm:

- Windows OS
- Python 3.6 or higher
- PyCharm (Community/Professional)
- Required Python packages:  
  `numpy`, `psutil`, `matplotlib`, `pandas`, `dash`, `plotly`, `scipy`

---

## 📁 Project Structure

```
fds_assignment2/
├── config.json                   # Configuration file
├── README.md                     # Project documentation
│
├── data/                         # Input datasets
│   ├── social_media_logs.txt      (File ommitted due to sise restrictions)
│   └── user_profiles.txt
│
├── output/                       # Generated outputs
│   ├── cleansed_data.txt         (File ommitted due to sise restrictions)
│   ├── user_activity.txt
│   ├── trending_content.txt
│   ├── skew_analysis.json
│   ├── joined_data.txt
│   └── workflow_summary.txt
│
├── src/                          # Source code
│   ├── cleansing_mapper.py
│   ├── cleansing_reducer.py
│   ├── action_aggregation_mapper.py
│   ├── action_aggregation_reducer.py
│   ├── trending_content_mapper.py
│   ├── trending_content_combiner.py
│   ├── trending_content_reducer.py
│   ├── join_activity_mapper.py
│   ├── join_profile_mapper.py
│   ├── join_reducer.py
│   ├── visualize_analytics.py
│   ├── skew_detection.py
│   └── memory_monitor.py
│
└── visualizations/               # Output visualizations
```

---

## 🚀 Quick Start

1. **Clone the repository**

2. **Install required libraries**
   ```bash
   pip install numpy psutil matplotlib pandas dash plotly scipy
   ```

3. **Unzip large files manually**  
   *(GitHub restricts uploads >100MB)*
   ```bash
   social_media_logs.zip  
   cleansed_data.zip
   ```

4. **Execute the workflow**
   ```bash
   python social_media_analytics_driver.py --config config.json
   ```

5. **Create static visualizations**
   ```bash
   python visualize_analytics.py --input-dir output --output-dir visualizations
   ```

6. **Launch the interactive dashboard**
   ```bash
   python analytics_dashboard.py
   ```

---

## 🔍 Components in Detail

### 1️⃣ Data Cleansing and Parsing

Processes the raw `social_media_logs.txt`:
- Parses records for: `Timestamp`, `UserID`, `ActionType`, `ContentID`, `Metadata`
- Validates timestamp formats and JSON structure
- Filters malformed entries, tracking drops

**Implementation Files**:
- `cleansing_mapper.py`
- `cleansing_reducer.py`

---

### 2️⃣ Action Aggregation and Sorting

Tracks activity counts per user:
- Posts, likes, comments, shares
- Sorted output by post count (descending)
- Uses composite keys for effective sorting

**Implementation Files**:
- `action_aggregation_mapper.py`
- `action_aggregation_reducer.py`

---

### 3️⃣ Trending Content Identification

Determines top-performing content:
- Calculates engagement = likes + shares
- Sets trend threshold via 90th percentile
- Handles skewed popular content

**Implementation Files**:
- `trending_content_mapper.py`
- `trending_content_combiner.py`
- `trending_content_reducer.py`

---

### 4️⃣ Dataset Joining

Merges logs and profiles:
- Reduce-side join strategy
- Handles power-user skew

**Implementation Files**:
- `join_activity_mapper.py`
- `join_profile_mapper.py`
- `join_reducer.py`
- `skew_detection.py`

---

### 5️⃣ Data Visualization

Visual summaries:
- Output Images

**Implementation Files**:
- `visualize_analytics.py` (static PNGs)

---

## ⚙️ Performance Optimizations

### 🔧 Code-Level
- **In-mapper combining** – Lowers shuffle volume
- **Efficient data structures**
- **Secondary sort logic**
- **Local combiners**

### 🖥️ System-Level
- **Skew detection**
- **Memory monitoring**
- **Dynamic reducer allocation**

---

## 🧪 Running Jobs Individually

Use `local_mapreduce.py` for component-wise execution:

```bash
# Run data cleansing
python local_mapreduce.py --job cleansing --input-dir data --output-dir output

# Run action aggregation
python local_mapreduce.py --job aggregation --input-dir output --output-dir output

# Run trending content detection
python local_mapreduce.py --job trending --input-dir output --output-dir output

# Run join operation
python local_mapreduce.py --job join --input-dir output --output-dir output
```

---

## 📊 Visualization Tools

### 📈 Static Graphs (`visualize_analytics.py`)

Generates:
- `activity_distribution.png` - Activity type breakdown
- `top_users.png` - Most active users
- `trending_content.png` - Top trending posts
- `activity_correlation.png` - Activity type heatmap
- `engagement_distribution.png` - Engagement histogram

Run with:
```bash
python visualize_analytics.py --input-dir output --output-dir visualizations
```

---

## 🛠️ Troubleshooting

- ✅ Check paths in `config.json`
- ✅ Confirm input files match expected format
- ✅ Validate Python environment and packages
- ✅ Review `workflow.log` for error details

---
