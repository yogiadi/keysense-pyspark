# KeySense (PySpark)

**KeySense** is an open-source PySpark utility for **detecting the record identity** (also known as the *grain*) of a dataset.  
It automatically scans combinations of columns (up to 4) and scores them for:

- **Uniqueness ratio** — How close the combo is to fully unique.
- **Drift stability** — Does it remain unique across partitions (e.g., by day)?
- **Null coverage** — How often any part of the combo is null.
- **Grain score** — Weighted score combining the above metrics.

## Why?

Most data quality frameworks check nulls, freshness, and schema — but they assume you already know your dataset’s grain.  
Grain detection is the *first step* in trusting your data.

**Without a declared grain, duplicates and "grain drift" can silently break KPIs, joins, and models.**

## Installation

Currently, install directly from source:

```bash
git clone https://github.com/yogiadi/keysense-pyspark.git
cd keysense-pyspark
```

## Quick Start

```python
from pyspark.sql import SparkSession
from keysense.profiler import KeySense

spark = SparkSession.builder.appName("KeySenseDemo").getOrCreate()

# Example DataFrame
data = [
    ("u1", "s1", "2025-08-01", "mobile"),
    ("u2", "s2", "2025-08-01", "desktop"),
    ("u1", "s1", "2025-08-02", "mobile"),
    ("u3", "s3", "2025-08-01", "mobile")
]
cols = ["user_id", "session_id", "event_date", "device_id"]
df = spark.createDataFrame(data, cols)

# Run KeySense
ks = KeySense(df, time_col="event_date", max_combo_len=3)
results = ks.evaluate()

# View top results
for r in results[:5]:
    print(r)
```

**Sample output:**
```text
{'combo': ('user_id', 'session_id', 'event_date'), 'uniqueness_ratio': 1.0, 'drift_stability': 0.98, 'null_ratio': 0.0, 'grain_score': 0.994, 'rows_scanned': 4, 'sample_fraction': 0.0}
{'combo': ('user_id', 'event_date'), 'uniqueness_ratio': 1.0, 'drift_stability': 0.95, 'null_ratio': 0.0, 'grain_score': 0.985, 'rows_scanned': 4, 'sample_fraction': 0.0}
...
```

## Roadmap
- [ ] Sampling for very large datasets
- [ ] Heuristics to prioritize likely ID columns
- [ ] CLI support
- [ ] Integration with Great Expectations / Soda

## Contributing
Issues and pull requests are welcome!  
Open an issue with ideas, bugs, or feature requests.

---

**Author:** Aditya Yogi  
**License:** MIT
