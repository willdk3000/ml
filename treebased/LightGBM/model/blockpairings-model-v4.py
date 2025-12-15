#!/usr/bin/env python3
"""
train_lightgbm_otp.py

Train a LightGBM binary classifier for on-time performance using a CSV.

Compatible with LightGBM >= 4.x
"""

import os
import json
import lightgbm as lgb
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score, log_loss

# -----------------------
# CONFIGURATION
# -----------------------
CSV_FILE = "../../data-samples/trips_oct_nov2025-var-p85.csv"
NUMERIC_COLUMNS = ['on_time_a', 'planned_layover_sec', 'p85_pct_b', 'planned_dur_b', 'range7525_b', 'ampeak_a', 'pmpeak_a']
CATEGORICAL_COLUMNS = ['route_pair']
LABEL_COLUMN = 'y_on_time_b'

TEST_SIZE = 0.2
RANDOM_SEED = 42

LGB_PARAMS = {
    'objective': 'binary',
    'boosting_type': 'gbdt',
    'metric': ['binary_logloss', 'auc'],
    'learning_rate': 0.05,
    'num_leaves': 127,
    'max_bin': 255,
    'min_data_in_leaf': 100,
    'bagging_fraction': 0.8,
    'bagging_freq': 1,
    'feature_fraction': 0.8,
    'verbosity': -1,
    'seed': RANDOM_SEED,
}

NUM_BOOST_ROUND = 2000
EARLY_STOPPING_ROUNDS = 50
MODEL_OUTPUT_DIR = "./blockpairings-model-v4"

# -----------------------
# HELPER FUNCTIONS
# -----------------------
def load_data(csv_path, numeric_cols, categorical_cols, label_col):
    usecols = list(set(numeric_cols + categorical_cols + [label_col]))
    df = pd.read_csv(csv_path, usecols=usecols)
    df[label_col] = pd.to_numeric(df[label_col], errors='coerce')
    df = df.dropna(subset=[label_col])
    df = df.dropna(subset=numeric_cols)
    for col in categorical_cols:
        df[col] = df[col].astype('category')
    print(f"Loaded {len(df)} rows from {csv_path}")
    return df

def prepare_datasets(df, numeric_cols, categorical_cols, label_col, test_size, random_seed):
    X = df[numeric_cols + categorical_cols]
    y = df[label_col].astype(int).values
    stratify = y if len(np.unique(y)) > 1 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_seed, stratify=stratify
    )
    return X_train, X_test, y_train, y_test

def make_lgb_dataset(X, y):
    return lgb.Dataset(X, label=y, categorical_feature='auto', free_raw_data=True)

# -----------------------
# MAIN
# -----------------------
def main():
    os.makedirs(MODEL_OUTPUT_DIR, exist_ok=True)

    # Load and prepare data
    df = load_data(CSV_FILE, NUMERIC_COLUMNS, CATEGORICAL_COLUMNS, LABEL_COLUMN)
    X_train, X_test, y_train, y_test = prepare_datasets(df, NUMERIC_COLUMNS, CATEGORICAL_COLUMNS, LABEL_COLUMN, TEST_SIZE, RANDOM_SEED)
    lgb_train = make_lgb_dataset(X_train, y_train)
    lgb_valid = make_lgb_dataset(X_test, y_test)

    # Train with callbacks for early stopping and evaluation logging
    print("Training LightGBM...")
    evals_result = {}
    model = lgb.train(
        params=LGB_PARAMS,
        train_set=lgb_train,
        num_boost_round=NUM_BOOST_ROUND,
        valid_sets=[lgb_train, lgb_valid],
        valid_names=['train', 'valid'],
        callbacks=[
            lgb.log_evaluation(period=50),
            lgb.early_stopping(stopping_rounds=EARLY_STOPPING_ROUNDS),
        ]
    )

    # Save model
    model_path = os.path.join(MODEL_OUTPUT_DIR, "lgbm_model.txt")
    model.save_model(model_path)
    print(f"Model saved to: {model_path}")

    # Save feature names
    feature_names = NUMERIC_COLUMNS + CATEGORICAL_COLUMNS
    with open(os.path.join(MODEL_OUTPUT_DIR, "feature_names.json"), "w") as fh:
        json.dump(feature_names, fh, indent=2)

    # Evaluate on test set
    print("Evaluating on test set...")
    y_prob = model.predict(X_test, num_iteration=model.best_iteration)
    y_pred = (y_prob >= 0.5).astype(int)
    auc = roc_auc_score(y_test, y_prob) if len(np.unique(y_test)) > 1 else float('nan')
    acc = accuracy_score(y_test, y_pred)
    ll = log_loss(y_test, y_prob)
    print(f"Test AUC: {auc:.4f}")
    print(f"Test Accuracy: {acc:.4f}")
    print(f"Test LogLoss: {ll:.6f}")

    # Feature importance
    importance_df = pd.DataFrame({
        'feature': model.feature_name(),
        'importance_gain': model.feature_importance(importance_type='gain'),
        'importance_split': model.feature_importance(importance_type='split')
    }).sort_values(by='importance_gain', ascending=False)
    importance_csv = os.path.join(MODEL_OUTPUT_DIR, "feature_importance.csv")
    importance_df.to_csv(importance_csv, index=False)
    print(f"Feature importance saved to: {importance_csv}")

    # Save evaluation results
    eval_log_path = os.path.join(MODEL_OUTPUT_DIR, "evals_result.json")
    with open(eval_log_path, "w") as fh:
        json.dump(evals_result, fh, indent=2)
    print(f"Eval results saved to: {eval_log_path}")

    print("Done.")

if __name__ == "__main__":
    main()
