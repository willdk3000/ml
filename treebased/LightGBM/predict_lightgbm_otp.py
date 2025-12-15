#!/usr/bin/env python3
"""
predict_lightgbm_otp.py

Run inference on a CSV using a trained LightGBM OTP model.
"""

import json
import lightgbm as lgb
import pandas as pd
import numpy as np
import os

# -----------------------
# CONFIGURATION
# -----------------------
MODEL_DIR = "./model/blockpairings-model-v4"
MODEL_PATH = os.path.join(MODEL_DIR, "lgbm_model.txt")
FEATURE_NAMES_PATH = os.path.join(MODEL_DIR, "feature_names.json")

INPUT_CSV = "./inference-data/predict-var-p85-pm-latea_oct-nov2025.csv"
OUTPUT_CSV = "./predictions/otp_predictions.csv"

PROBABILITY_COLUMN = "prob_on_time_b"
PREDICTION_COLUMN = "pred_on_time_b"
THRESHOLD = 0.5

# -----------------------
# LOAD MODEL & METADATA
# -----------------------
print("Loading model...")
model = lgb.Booster(model_file=MODEL_PATH)

with open(FEATURE_NAMES_PATH) as fh:
    FEATURE_NAMES = json.load(fh)

NUMERIC_COLUMNS = [
    c for c in FEATURE_NAMES
    if c not in ["route_pair"]
]
CATEGORICAL_COLUMNS = ["route_pair"]

# -----------------------
# LOAD INPUT DATA
# -----------------------
print(f"Loading inference data: {INPUT_CSV}")
df = pd.read_csv(INPUT_CSV)

# -----------------------
# VALIDATION
# -----------------------
missing = set(FEATURE_NAMES) - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# Enforce correct column order
df = df[FEATURE_NAMES]

# Numeric coercion
for col in NUMERIC_COLUMNS:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Drop rows with invalid numeric values
before = len(df)
df = df.dropna(subset=NUMERIC_COLUMNS)
after = len(df)
if after < before:
    print(f"Dropped {before - after} rows due to invalid numeric values")

# Categorical casting (critical)
for col in CATEGORICAL_COLUMNS:
    df[col] = df[col].astype("category")

# -----------------------
# RUN INFERENCE
# -----------------------
print("Running predictions...")
y_prob = model.predict(df, num_iteration=model.best_iteration)

df[PROBABILITY_COLUMN] = y_prob
df[PREDICTION_COLUMN] = (y_prob >= THRESHOLD).astype(int)

# -----------------------
# SAVE OUTPUT
# -----------------------
df.to_csv(OUTPUT_CSV, index=False)
print(f"Predictions saved to: {OUTPUT_CSV}")

print("Done.")
