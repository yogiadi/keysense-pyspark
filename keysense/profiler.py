# keysense/profiler.py
# [v0.4.2 CHANGE] Upgraded KeySense implementation:
#   - Adds near_key_gap metric
#   - Adds JSON export (--emit-json)
#   - Adds Great Expectations export (--emit-ge)
#   - Uses approx_count_distinct for scalability
#   - Adds pruning heuristics and candidate column profiling

from __future__ import annotations
from typing import List, Tuple, Iterable, Optional
from itertools import combinations
import argparse, json, os

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F


def _hash_combo(cols: List[str]) -> F.Column:
    """[v0.4.2 NEW] Stable hash of a column combo for distinct counting."""
    safe = [F.coalesce(F.col(c).cast("string"), F.lit("∅")) for c in cols]
    return F.sha2(F.concat_ws("||", *safe), 256)


def _date_bucket(col: F.Column, grain: str) -> F.Column:
    """[v0.4.2 NEW] Normalize timestamps into day/week/month buckets."""
    grain = (grain or "day").lower()
    if grain == "day":
        return F.to_date(col)
    if grain == "week":
        return F.date_sub(F.next_day(F.date_sub(F.to_date(col), 1), "Mon"), 7)
    if grain == "month":
        return F.trunc(col, "month")
    return F.to_date(col)


class KeySense:
    """
    KeySense v0.4.2

    [v0.4.2 CHANGE]
    - Computes uniqueness, stability, null coverage
    - Adds near_key_gap = 1 - (uniqueness * null_coverage)
    - Weighted grain_score = 0.6*uniqueness + 0.25*stability + 0.15*nulls
    - Supports --emit-json and --emit-ge for exports
    """

    def __init__(
        self,
        df: DataFrame,
        time_col: str = "event_date",
        max_combo_len: int = 4,
        sample_fraction: Optional[float] = None,
        sample_seed: int = 42,
        approx_rsd: float = 0.02,
        weight_uniqueness: float = 0.6,
        weight_stability: float = 0.25,
        weight_nulls: float = 0.15,
        min_col_cardinality: int = 10,
        max_null_fraction: float = 0.5,
        time_grain: str = "day",
        candidate_cols_include: Optional[List[str]] = None,
        candidate_cols_exclude: Optional[List[str]] = None,
    ) -> None:
        self.df = df
        self.time_col = time_col
        self.max_combo_len = max_combo_len
        self.sample_fraction = sample_fraction
        self.sample_seed = sample_seed
        self.approx_rsd = approx_rsd
        self.w_uni = weight_uniqueness
        self.w_stab = weight_stability
        self.w_null = weight_nulls
        self.min_col_card = min_col_cardinality
        self.max_null_frac = max_null_fraction
        self.time_grain = time_grain
        self.cand_include = set(candidate_cols_include or [])
        self.cand_exclude = set(candidate_cols_exclude or [])

        # [v0.4.2 NEW] Optional sampling
        if self.sample_fraction and (0 < self.sample_fraction < 1):
            self.df = self.df.sample(
                withReplacement=False, fraction=self.sample_fraction, seed=self.sample_seed
            )

        self.total_rows = self.df.count()
        if self.total_rows == 0:
            raise ValueError("Input DataFrame has 0 rows after filtering/sampling.")

        self.has_time = self.time_col in self.df.columns
        if self.has_time:
            self.df = self.df.withColumn(
                "__ks_time_bucket", _date_bucket(F.col(self.time_col), self.time_grain)
            )

        self.profile_df = self._profile_columns()
        self.candidate_cols = self._select_candidate_columns()

    # ---------- column profiling ----------

    def _profile_columns(self) -> DataFrame:
        """[v0.4.2 NEW] Profile columns with null_fraction and approx distinct."""
        stats = []
        dtypes = dict(self.df.dtypes)
        for c in self.df.columns:
            if c in {"__ks_time_bucket"}:
                continue
            dtype = dtypes.get(c, "")
            if dtype.startswith(("array", "map", "struct")):
                continue
            row = self.df.select(
                F.count(F.when(F.col(c).isNull(), 1)).alias("nulls"),
                F.approx_count_distinct(F.col(c), rsd=self.approx_rsd).alias("approx_distinct"),
                F.count(F.lit(1)).alias("total"),
            ).first()
            stats.append((c, dtype, int(row["approx_distinct"]),
                          float(row["nulls"]) / float(row["total"])))
        spark = self.df.sparkSession
        return spark.createDataFrame(
            stats, schema=["column","dtype","approx_distinct","null_fraction"]
        ).orderBy(F.desc("approx_distinct"))

    def _select_candidate_columns(self) -> List[str]:
        """[v0.4.2 NEW] Drop low-cardinality and high-null columns."""
        cols = []
        for row in self.profile_df.collect():
            c = row["column"]
            null_frac = float(row["null_fraction"])
            card = int(row["approx_distinct"])
            if c in self.cand_exclude: continue
            if self.cand_include and c not in self.cand_include: continue
            if card < self.min_col_card: continue
            if null_frac > self.max_null_frac: continue
            cols.append(c)
        return cols

    # ---------- combo evaluation ----------

    def _generate_combos(self) -> Iterable[Tuple[str, ...]]:
        """[v0.4.2 ENHANCED] Yield column combos up to max_combo_len."""
        level = [(c,) for c in self.candidate_cols]
        for combo in level: yield combo
        k=2; current=level
        while k <= self.max_combo_len and current:
            candidates=set()
            for i in range(len(current)):
                for j in range(i+1,len(current)):
                    merged=tuple(sorted(set(current[i])|set(current[j])))
                    if len(merged)==k: candidates.add(merged)
            current=list(candidates)
            for combo in current: yield combo
            k+=1

    def _null_coverage(self, combo: Tuple[str, ...]) -> float:
        cond = F.lit(True)
        for c in combo: cond = cond & F.col(c).isNotNull()
        hits = self.df.filter(cond).count()
        return float(hits) / float(self.total_rows)

    def _stability_score(self, combo: Tuple[str, ...]):
        if not self.has_time: return 0.0, 1.0
        h = _hash_combo(list(combo)).alias("h")
        per = self.df.groupBy("__ks_time_bucket").agg(
            F.approx_count_distinct(h, rsd=self.approx_rsd).alias("d"))
        stats = per.agg(F.avg("d").alias("mean"), F.stddev_pop("d").alias("std")).first()
        mean = float(stats["mean"]) if stats["mean"] else 0.0
        std = float(stats["std"]) if stats["std"] else 0.0
        if mean <= 0.0: return 0.0, 1.0
        cv = std / mean
        return cv, 1.0 / (1.0 + cv)

    def _uniqueness(self, combo: Tuple[str, ...]):
        h = _hash_combo(list(combo)).alias("h")
        approx_d = int(self.df.select(
            F.approx_count_distinct(h, rsd=self.approx_rsd).alias("d")).first()["d"])
        uniq = min(float(approx_d) / float(self.total_rows), 1.0)
        return approx_d, uniq

    def evaluate(self, topk: int = 50) -> DataFrame:
        """[v0.4.2 ENHANCED] Evaluate combos and return Spark DataFrame of metrics."""
        rows=[]
        for combo in self._generate_combos():
            approx_d, uniq = self._uniqueness(combo)
            null_cov = self._null_coverage(combo)
            cv, stab = self._stability_score(combo)
            uniq_adj = uniq * null_cov
            grain_score = self.w_uni*uniq_adj + self.w_stab*stab + self.w_null*null_cov
            near_key_gap = 1.0 - uniq_adj
            rows.append((list(combo), len(combo), approx_d, self.total_rows,
                         uniq, null_cov, cv, stab, grain_score, near_key_gap, self.time_grain))
        spark = self.df.sparkSession
        return spark.createDataFrame(rows, schema=[
            "combo","combo_len","approx_distinct","total_rows","uniqueness_ratio",
            "null_coverage","stability_cv","stability_score","grain_score",
            "near_key_gap","time_grain"
        ]).orderBy(F.desc("grain_score")).limit(topk)

# [v0.4.2 NEW CLI SUPPORT] for running from command line
def build_spark(app_name="KeySense") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

def parse_args():
    p=argparse.ArgumentParser()
    p.add_argument("--input",required=True)
    p.add_argument("--format",choices=["parquet","csv","table"],default="parquet")
    p.add_argument("--time-col",default="event_date")
    p.add_argument("--time-grain",choices=["day","week","month"],default="day")
    p.add_argument("--max-combo-len",type=int,default=4)
    p.add_argument("--sample",type=float,default=None)
    p.add_argument("--approx-rsd",type=float,default=0.02)
    p.add_argument("--min-col-card",type=int,default=10)
    p.add_argument("--max-null-frac",type=float,default=0.5)
    p.add_argument("--weights",type=str,default="0.6,0.25,0.15")
    p.add_argument("--topk",type=int,default=50)
    p.add_argument("--output",required=True)
    p.add_argument("--emit-json",type=str,default=None)
    p.add_argument("--json-indent",type=int,default=2)
    p.add_argument("--emit-ge",type=str,default=None)
    return p.parse_args()

def main():
    args=parse_args()
    spark=build_spark()
    if args.format=="parquet": df=spark.read.parquet(args.input)
    elif args.format=="csv": df=spark.read.option("header",True).csv(args.input)
    elif args.format=="table": df=spark.table(args.input)
    else: raise ValueError("bad format")

    w_uni,w_stab,w_null=[float(x) for x in args.weights.split(",")]
    ks=KeySense(df,
                time_col=args.time_col,
                max_combo_len=args.max_combo_len,
                sample_fraction=args.sample,
                approx_rsd=args.approx_rsd,
                weight_uniqueness=w_uni,
                weight_stability=w_stab,
                weight_nulls=w_null,
                min_col_cardinality=args.min_col_card,
                max_null_fraction=args.max_null_frac,
                time_grain=args.time_grain)

    out=ks.evaluate(topk=args.topk)
    out.write.mode("overwrite").parquet(args.output)

    if args.emit_json:
        rows=[r.asDict(recursive=True) for r in out.collect()]
        with open(args.emit_json,"w") as f: json.dump(rows,f,indent=args.json_indent)
        print("✓ JSON export written to:",args.emit_json)

    if args.emit_ge:
        os.makedirs(args.emit_ge,exist_ok=True)
        top1=out.limit(1).collect()[0]
        cols=top1.combo
        exp={
            "expectations":[
                {"expectation_type":"expect_compound_columns_to_be_unique",
                 "kwargs":{"column_list":cols}},
                *[{"expectation_type":"expect_column_values_to_not_be_null",
                   "kwargs":{"column":c}} for c in cols]
            ]
        }
        path=os.path.join(args.emit_ge,"expectations.json")
        with open(path,"w") as f: json.dump(exp,f,indent=2)
        print("✓ Great Expectations export written to:",path)

    print("✓ KeySense v0.4.2 results written to:",args.output)

if __name__=="__main__":
    main()
