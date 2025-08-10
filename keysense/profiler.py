# keysense/profiler.py
from __future__ import annotations
from itertools import combinations
from typing import Dict, Iterable, List, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, count, countDistinct, approx_count_distinct,
    isnan, when, lit
)


class KeySense:
    """
    KeySense: Detect & score candidate row-identity (grain) keys for a PySpark DataFrame.

    For each 1..max_combo_len column combination, computes:
      - uniqueness_ratio     = count(distinct combo) / total_rows
      - drift_stability      = avg over partitions (e.g., by day) of distinct/total
      - combo_null_ratio     = rows with ANY NULL in combo / total_rows
      - grain_score          = 0.50*uniqueness + 0.30*stability + 0.20*(1-null_ratio)

    Notes:
      - Supports optional sampling for big DataFrames via sample_fraction.
      - Light heuristics prune columns before combo generation.
      - If time_col not present, stability falls back to uniqueness_ratio.

    Example:
        ks = KeySense(df, time_col="event_date", max_combo_len=4, sample_fraction=0.0)
        results = ks.evaluate()
    """

    def __init__(
        self,
        df: DataFrame,
        *,
        time_col: str = "event_date",
        max_combo_len: int = 3,
        sample_fraction: float = 0.0,   # 0.0 = no sampling; else (0,1]
        seed: int = 42,
        null_ratio_cap: float = 0.6,    # drop columns with >60% nulls
        max_combos: int = 5000          # hard cap to avoid explosion
    ):
        if max_combo_len < 1 or max_combo_len > 4:
            raise ValueError("max_combo_len must be between 1 and 4.")
        if sample_fraction < 0.0 or sample_fraction > 1.0:
            raise ValueError("sample_fraction must be in [0.0, 1.0].")

        self.orig_df = df
        self.time_col = time_col
        self.max_combo_len = max_combo_len
        self.sample_fraction = sample_fraction
        self.seed = seed
        self.null_ratio_cap = null_ratio_cap
        self.max_combos = max_combos

        self.df = (
            df.sample(False, sample_fraction, seed) if 0.0 < sample_fraction < 1.0 else df
        )
        self.total_rows = self.df.count()
        if self.total_rows == 0:
            raise ValueError("DataFrame has zero rows after sampling.")

        self.columns = [c for c in self.df.columns]
        self._col_profile: Dict[str, Dict[str, float]] = {}

    # ---------- public API ----------

    def evaluate(self) -> List[Dict]:
        """Return a ranked list of candidate key combos with metrics & score."""
        self._profile_columns()
        cand_cols = self._select_candidate_columns()
        combos = self._generate_combos(cand_cols)

        results: List[Dict] = []
        for combo in combos:
            try:
                ur = self._uniqueness_ratio(combo)
                dr = self._drift_stability(combo)
                nr = self._combo_null_ratio(combo)
                score = 0.50 * ur + 0.30 * dr + 0.20 * (1.0 - nr)
                results.append({
                    "combo": tuple(combo),
                    "uniqueness_ratio": round(ur, 6),
                    "drift_stability": round(dr, 6),
                    "null_ratio": round(nr, 6),
                    "grain_score": round(score, 6),
                    "rows_scanned": self.total_rows,
                    "sample_fraction": self.sample_fraction
                })
            except Exception as e:
                # Skip pathological combos (e.g., bad types) but keep going
                results.append({
                    "combo": tuple(combo),
                    "error": str(e)
                })

        # Sort by score desc, then by combo length asc (prefer simpler keys)
        results = sorted(
            results,
            key=lambda r: (-r.get("grain_score", -1.0), len(r.get("combo", ())))
        )
        return results

    # ---------- profiling & heuristics ----------

    def _profile_columns(self) -> None:
        """Compute per-column null_ratio and distinct_ratio."""
        # Nulls per column
        null_counts = {
            c: self.df.filter(col(c).isNull() | isnan(col(c))).count() for c in self.columns
        }

        # Distinct counts (approx to be fast, exact for very small tables)
        approx = self.total_rows > 1_000_000
        distinct_counts = {}
        for c in self.columns:
            if approx:
                distinct_counts[c] = self.df.agg(approx_count_distinct(col(c))).first()[0]
            else:
                distinct_counts[c] = self.df.select(c).distinct().count()

        # Store profile
        for c in self.columns:
            self._col_profile[c] = {
                "null_ratio": null_counts[c] / self.total_rows,
                "distinct_ratio": max(1, distinct_counts[c]) / self.total_rows
            }

    def _select_candidate_columns(self) -> List[str]:
        """
        Heuristically prune columns before generating combos:
          - drop columns with too many nulls
          - prefer columns with some variety (distinct_ratio >= 0.01)
          - lightly boost columns that look like IDs
        """
        def id_boost(name: str) -> float:
            lname = name.lower()
            hints = ["id", "uuid", "key", "number", "code", "hash"]
            return 0.1 if any(h in lname for h in hints) else 0.0

        scored = []
        for c in self.columns:
            p = self._col_profile[c]
            if p["null_ratio"] <= self.null_ratio_cap and p["distinct_ratio"] >= 0.01:
                base = p["distinct_ratio"] - p["null_ratio"]
                scored.append((c, base + id_boost(c)))

        # Rank and keep a reasonable top slice to limit combo explosion
        ranked = [c for c, _ in sorted(scored, key=lambda x: -x[1])]
        # Keep at most 20 columns to keep combinations tractable
        return ranked[:20]

    def _generate_combos(self, cols: Sequence[str]) -> List[List[str]]:
        """Generate combos up to max_combo_len with a hard cap."""
        all_combos: List[List[str]] = []
        for r in range(1, self.max_combo_len + 1):
            for combo in combinations(cols, r):
                all_combos.append(list(combo))
                if len(all_combos) >= self.max_combos:
                    return all_combos
        return all_combos

    # ---------- metrics ----------

    def _combo_null_ratio(self, cols: Sequence[str]) -> float:
        cond = None
        for c in cols:
            expr = col(c).isNull() | isnan(col(c))
            cond = expr if cond is None else (cond | expr)
        nulls = self.df.filter(cond).count() if cond is not None else 0
        return nulls / self.total_rows

    def _uniqueness_ratio(self, cols: Sequence[str]) -> float:
        # For big data, approx distinct can be a toggle later; use exact for clarity
        dcount = self.df.select(*[col(c) for c in cols]).distinct().count()
        return dcount / self.total_rows

    def _drift_stability(self, cols: Sequence[str]) -> float:
        if self.time_col not in self.columns:
            # Fall back to uniqueness when no time partition column available
            return self._uniqueness_ratio(cols)

        by = self.df.groupBy(col(self.time_col)) \
                    .agg(
                        count(lit(1)).alias("total"),
                        countDistinct(*[col(c) for c in cols]).alias("distinct")
                    ) \
                    .withColumn("ratio", col("distinct") / col("total"))

        parts = [row["ratio"] for row in by.collect()]
        return (sum(parts) / len(parts)) if parts else 0.0
