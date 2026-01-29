# Heart Disease Analysis with PySpark

A comprehensive heart disease data analysis using Apache Spark's PySpark library. This project performs various statistical analyses on heart disease patient data including age distribution, gender analysis, chest pain types, blood pressure, cholesterol levels, and correlation analysis.

## 📊 Features

- **Dataset Overview**: Basic statistics and schema information
- **Heart Disease Distribution**: Binary classification analysis (Presence/Absence)
- **Age Analysis**: Statistical analysis by heart disease status
- **Gender Analysis**: Distribution and percentages by gender
- **Chest Pain Type Analysis**: Analysis of different chest pain categories
- **Blood Pressure Analysis**: BP statistics and categorization
- **Cholesterol Analysis**: Cholesterol levels with clinical categorization
- **Risk Factor Analysis**: Combined risk factors analysis
- **Correlation Analysis**: Feature correlations with heart disease
- **Age Group Analysis**: Risk stratification by age groups

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Apache Spark 3.3 or higher
- Java 8 or 11 (required for Spark)

### Installation

#### 1. Install Java (if not installed)

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

**macOS (using Homebrew):**
```bash
brew install openjdk@11
```

**Windows:**
Download and install from [Adoptium](https://adoptium.net/)

#### 2. Install PySpark

```bash
pip install pyspark
```

Or with specific version:
```bash
pip install pyspark==3.5.0
```

#### 3. Verify Installation

```bash
python -c "from pyspark.sql import SparkSession; print('PySpark installed successfully!')"
```

### Data Setup

1. Download the Heart Disease Prediction dataset from [Kaggle](https://www.kaggle.com/datasets)
2. Place the file `Heart_Disease_Prediction.csv` in your data directory
3. Update the `data_path` variable in `heart_dieases_analysis.py` to point to your data location

```python
data_path = "/path/to/your/Heart_Disease_Prediction.csv"
```

## 📁 Project Structure

```
pyspark/
├── heart_dieases_analysis.py    # Main analysis script
├── test.py                       # Simple PySpark example
├── README.md                     # This file
└── heart_analysis_results/       # Output directory (created after running)
```

## 🏃 Running the Analysis

### Run the Main Analysis

```bash
python heart_dieases_analysis.py
```

### Run the Test Script

```bash
python test.py
```

## 📋 Requirements

```
# Core Requirements
pyspark>=3.3.0

# Python Version
python>=3.8

# Java Version (for Spark)
java>=8, <=11
```

### Complete requirements.txt

```txt
pyspark>=3.5.0
```

### Install All Dependencies

```bash
pip install -r requirements.txt
```

## 🔧 Configuration

### Spark Configuration

The application uses the following default Spark settings:

```python
spark = SparkSession.builder \
    .appName("Heart Disease Analysis") \
    .getOrCreate()
```

### Data Path

Update the data path in `heart_dieases_analysis.py`:

```python
data_path = "/home/kalamay/Downloads/archive/Heart_Disease_Prediction.csv"
```

## 📊 Dataset Columns

| Column | Description |
|--------|-------------|
| Age | Patient age in years |
| Sex | Gender (1=Male, 0=Female) |
| Chest pain type | 1-4 (Typical Angina, Atypical Angina, Non-Anginal Pain, Asymptomatic) |
| BP | Blood Pressure (mm Hg) |
| Cholesterol | Serum Cholesterol (mg/dl) |
| FBS over 120 | Fasting Blood Sugar > 120 mg/dl (1=Yes, 0=No) |
| Max HR | Maximum Heart Rate achieved |
| Exercise angina | Exercise induced angina (1=Yes, 0=No) |
| ST depression | ST depression induced by exercise |
| Heart Disease | Target variable (Presence/Absence) |

## 📝 Output

The analysis saves results to the `heart_analysis_results/` directory:

- `summary/` - Summary statistics CSV
- `cleaned_data/` - Processed data with binary target variable

## 🔍 Analysis Examples

### Sample Output

```
=== Heart Disease Distribution ===
+------------+-----+
|Heart Disease|count|
+------------+-----+
|    Absence|  150|
|  Presence|  120|
+------------+-----+

=== Age Analysis by Heart Disease ===
+------------+-------+-------+-------+-------------+
|Heart Disease|avg_age|min_age|max_age|patient_count|
+------------+-------+-------+-------+-------------+
|    Absence|  52.3 |   29  |   76  |     150     |
|  Presence|  56.8 |   35  |   77  |     120     |
+------------+-------+-------+-------+-------------+
```

## 🛠️ Troubleshooting

### Common Issues

1. **Java not found**:
   ```bash
   export JAVA_HOME=/path/to/java
   ```

2. **Memory issues**:
   ```python
   spark = SparkSession.builder \
       .config("spark.driver.memory", "4g") \
       .getOrCreate()
   ```

3. **Hadoop_HOME error**:
   ```python
   import os
   os.environ["HADOOP_HOME"] = "/path/to/hadoop"
   ```

## 📚 Dependencies Explained

- **PySpark**: Apache Spark Python API for distributed data processing
- **Java**: Required runtime for Spark (version 8 or 11 recommended)
- **Python**: Programming language (3.8+)

## 📄 License

This project is open source and available for educational and research purposes.

## 🤝 Contributing

Feel free to fork this project and add more analyses or improvements!

## 📞 Support

For issues or questions, please open an issue in the repository.

---

**Note**: Ensure that the Heart_Disease_Prediction.csv file is properly formatted with headers matching the column names expected in the analysis code and put the correct path in path in starting of code.

