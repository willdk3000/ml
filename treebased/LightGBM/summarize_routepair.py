#!/usr/bin/env python3
"""
aggregate_predictions_by_routedir.py

Aggregate LightGBM OTP predictions by routedir.
"""

import pandas as pd
import os

# -----------------------
# CONFIGURATION
# -----------------------
ORIGINAL_CSV = "../data-samples/trips_oct_nov2025-var-p85.csv"
PREDICTIONS_CSV = "./predictions/otp_predictions.csv"
OUTPUT_CSV = "./predictions/routepair_summary.csv"

ROUTEDIR_COLUMN = "route_pair"
PROB_COLUMN = "prob_on_time_b"
CLASS_COLUMN = "pred_on_time_b"

# -----------------------
# LOAD DATA
# -----------------------
print("Loading original data...")
df_orig = pd.read_csv(ORIGINAL_CSV, usecols=[ROUTEDIR_COLUMN])

print("Loading prediction results...")
df_pred = pd.read_csv(PREDICTIONS_CSV, usecols=[
    ROUTEDIR_COLUMN,
    PROB_COLUMN,
    CLASS_COLUMN
])

# -----------------------
# VALIDATION
# -----------------------
missing = set(df_orig.columns) ^ {ROUTEDIR_COLUMN}
if missing:
    raise ValueError(f"Unexpected columns in original CSV: {missing}")

missing = {ROUTEDIR_COLUMN, PROB_COLUMN, CLASS_COLUMN} - set(df_pred.columns)
if missing:
    raise ValueError(f"Missing columns in prediction CSV: {missing}")

# -----------------------
# AGGREGATION
# -----------------------

# Count occurrences in original data
counts = (
    df_orig
    .groupby(ROUTEDIR_COLUMN)
    .size()
    .rename("n_obs")
)

# Aggregate predictions
pred_stats = (
    df_pred
    .groupby(ROUTEDIR_COLUMN)
    .agg(
        mean_prob_on_time_b=(PROB_COLUMN, "mean"),
        pct_pred_on_time_b=(CLASS_COLUMN, "mean")
    )
)

# Combine
result = (
    counts
    .to_frame()
    .join(pred_stats, how="left")
    .reset_index()
)

# -----------------------
# SAVE RESULT
# -----------------------
result.to_csv(OUTPUT_CSV, index=False)
print(f"Saved aggregated results to: {OUTPUT_CSV}")

print("Done.")
