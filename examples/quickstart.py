#!/usr/bin/env python3
from pyspark.sql import SparkSession
try:
    from keysense import KeySense  # cleaner import
except ImportError:
    from keysense.profiler import KeySense  # fallback if __init__ not updated

spark = SparkSession.builder.appName("KeySenseQuickstart").getOrCreate()

data = [
    ("u1", "s1", "2025-08-01", "mobile"),
    ("u2", "s2", "2025-08-01", "desktop"),
    ("u1", "s1", "2025-08-02", "mobile"),
    ("u3", "s3", "2025-08-01", "mobile"),
]
cols = ["user_id", "session_id", "event_date", "device_id"]
df = spark.createDataFrame(data, cols)

ks = KeySense(df, time_col="event_date", max_combo_len=3)
results = ks.evaluate()

print("Top candidate keys:\n")
for r in results[:5]:
    print(r)

spark.stop()
