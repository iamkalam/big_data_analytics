from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

print("=== Heart Disease Analysis ===")

# Create SparkSession
spark = SparkSession.builder \
    .appName("Heart Disease Analysis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read your data
data_path = "/home/kalamay/Downloads/archive/Heart_Disease_Prediction.csv" # Update with correct path if needed
heart_df = spark.read.csv(data_path, header=True, inferSchema=True)

print("=== Dataset Overview ===")
print(f"Rows: {heart_df.count():,}")
print(f"Columns: {len(heart_df.columns)}")

print("\n=== Column Names ===")
for i, col in enumerate(heart_df.columns, 1):
    print(f"{i:2}. {col}")

print("\n=== Schema ===")
heart_df.printSchema()

print("\n=== First 5 Rows ===")
heart_df.show(5, truncate=False)

print("\n=== Summary Statistics ===")
heart_df.describe().show()

# ========== ANALYSIS FOR YOUR SPECIFIC COLUMNS ==========

# 1. Heart Disease Distribution
print("\n=== Heart Disease Distribution ===")
heart_df.groupBy("Heart Disease").count().orderBy("Heart Disease").show()

# Convert to binary if needed (Presence = 1, Absence = 0)
heart_df = heart_df.withColumn("target", 
    when(col("Heart Disease") == "Presence", 1).otherwise(0))

print("\n=== Binary Target Distribution ===")
heart_df.groupBy("target", "Heart Disease").count().show()

# 2. Age Analysis by Heart Disease
print("\n=== Age Analysis by Heart Disease ===")
heart_df.createOrReplaceTempView("heart_data")

spark.sql("""
    SELECT 
        `Heart Disease`,
        ROUND(AVG(Age), 2) as avg_age,
        MIN(Age) as min_age,
        MAX(Age) as max_age,
        COUNT(*) as patient_count
    FROM heart_data
    GROUP BY `Heart Disease`
    ORDER BY `Heart Disease`
""").show()

# 3. Gender Analysis
print("\n=== Gender Distribution by Heart Disease ===")
spark.sql("""
    SELECT 
        `Heart Disease`,
        CASE 
            WHEN Sex = 1 THEN 'Male'
            WHEN Sex = 0 THEN 'Female'
            ELSE 'Unknown'
        END as gender,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY `Heart Disease`), 2) as percentage
    FROM heart_data
    GROUP BY `Heart Disease`, Sex
    ORDER BY `Heart Disease`, gender
""").show()

# 4. Chest Pain Type Analysis
print("\n=== Chest Pain Type Analysis ===")
chest_pain_map = {
    1: "Typical Angina",
    2: "Atypical Angina", 
    3: "Non-Anginal Pain",
    4: "Asymptomatic"
}

# Register UDF for mapping chest pain types
from pyspark.sql.types import StringType

def map_chest_pain(cp):
    return chest_pain_map.get(cp, "Unknown")

map_chest_pain_udf = udf(map_chest_pain, StringType())

heart_df_with_cp = heart_df.withColumn("Chest_Pain_Type", map_chest_pain_udf(col("Chest pain type")))
heart_df_with_cp.createOrReplaceTempView("heart_data_cp")

spark.sql("""
    SELECT 
        `Heart Disease`,
        Chest_Pain_Type,
        COUNT(*) as count,
        ROUND(AVG(Age), 2) as avg_age
    FROM heart_data_cp
    GROUP BY `Heart Disease`, Chest_Pain_Type
    ORDER BY `Heart Disease`, count DESC
""").show()

# 5. Blood Pressure Analysis
print("\n=== Blood Pressure Analysis ===")
spark.sql("""
    SELECT 
        `Heart Disease`,
        ROUND(AVG(BP), 2) as avg_bp,
        MIN(BP) as min_bp,
        MAX(BP) as max_bp,
        CASE 
            WHEN AVG(BP) < 120 THEN 'Normal'
            WHEN AVG(BP) BETWEEN 120 AND 129 THEN 'Elevated'
            WHEN AVG(BP) BETWEEN 130 AND 139 THEN 'High Stage 1'
            ELSE 'High Stage 2'
        END as bp_category
    FROM heart_data
    GROUP BY `Heart Disease`
""").show()

# 6. Cholesterol Analysis
print("\n=== Cholesterol Analysis ===")
spark.sql("""
    SELECT 
        `Heart Disease`,
        ROUND(AVG(Cholesterol), 2) as avg_cholesterol,
        CASE 
            WHEN AVG(Cholesterol) < 200 THEN 'Desirable'
            WHEN AVG(Cholesterol) BETWEEN 200 AND 239 THEN 'Borderline High'
            ELSE 'High'
        END as cholesterol_category,
        COUNT(*) as patient_count
    FROM heart_data
    GROUP BY `Heart Disease`
""").show()

# 7. Multiple Factors Analysis
print("\n=== Risk Factor Combination Analysis ===")
spark.sql("""
    SELECT 
        `Heart Disease`,
        CASE WHEN BP > 140 THEN 'High BP' ELSE 'Normal BP' END as bp_status,
        CASE WHEN Cholesterol > 240 THEN 'High Cholesterol' ELSE 'Normal Cholesterol' END as chol_status,
        CASE WHEN `FBS over 120` = 1 THEN 'High FBS' ELSE 'Normal FBS' END as fbs_status,
        COUNT(*) as patient_count,
        ROUND(AVG(Age), 2) as avg_age
    FROM heart_data
    GROUP BY `Heart Disease`, bp_status, chol_status, fbs_status
    HAVING COUNT(*) >= 5  -- Only show combinations with at least 5 patients
    ORDER BY patient_count DESC
    LIMIT 10
""").show()

# 8. Correlation Analysis
print("\n=== Correlation with Heart Disease ===")
numeric_cols = ["Age", "Sex", "BP", "Cholesterol", "Max HR", "ST depression"]

for col_name in numeric_cols:
    if col_name in heart_df.columns:
        try:
            corr = heart_df.stat.corr("target", col_name)
            print(f"{col_name}: {corr:.3f}")
        except:
            print(f"{col_name}: Could not calculate correlation")

# 9. Advanced Analysis: Age Groups
print("\n=== Age Group Analysis ===")
spark.sql("""
    SELECT 
        `Heart Disease`,
        CASE 
            WHEN Age < 40 THEN '<40'
            WHEN Age BETWEEN 40 AND 49 THEN '40-49'
            WHEN Age BETWEEN 50 AND 59 THEN '50-59'
            WHEN Age BETWEEN 60 AND 69 THEN '60-69'
            ELSE '70+'
        END as age_group,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY age_group), 2) as disease_percentage
    FROM heart_data
    GROUP BY `Heart Disease`, age_group
    ORDER BY age_group, `Heart Disease`
""").show()

# 10. Create a comprehensive summary
print("\n=== Comprehensive Summary ===")
summary_df = spark.sql("""
    SELECT 
        `Heart Disease` as disease_status,
        COUNT(*) as total_patients,
        ROUND(AVG(Age), 2) as avg_age,
        ROUND(AVG(BP), 2) as avg_bp,
        ROUND(AVG(Cholesterol), 2) as avg_cholesterol,
        ROUND(AVG(`Max HR`), 2) as avg_max_hr,
        ROUND(AVG(`ST depression`), 3) as avg_st_depression,
        SUM(CASE WHEN Sex = 1 THEN 1 ELSE 0 END) as male_count,
        SUM(CASE WHEN Sex = 0 THEN 1 ELSE 0 END) as female_count,
        SUM(CASE WHEN `Exercise angina` = 1 THEN 1 ELSE 0 END) as exercise_angina_count,
        SUM(CASE WHEN `FBS over 120` = 1 THEN 1 ELSE 0 END) as high_fbs_count
    FROM heart_data
    GROUP BY `Heart Disease`
    ORDER BY `Heart Disease`
""")

summary_df.show(truncate=False)

# Save results
print("\n=== Saving Results ===")
output_dir = "heart_analysis_results"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Save summary
summary_df.coalesce(1).write.csv(f"{output_dir}/summary", header=True, mode="overwrite")

# Save cleaned data with binary target
heart_df.coalesce(1).write.csv(f"{output_dir}/cleaned_data", header=True, mode="overwrite")

print(f"Results saved to: {output_dir}/")

# Stop Spark
spark.stop()
print("\n=== Analysis Complete ===")