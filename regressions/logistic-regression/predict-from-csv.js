import fs from 'fs';
import { parse } from 'csv-parse/sync';
import * as tf from '@tensorflow/tfjs-node';

/**
 * CONFIG — CHANGE THESE
 */
const MODEL_PATH = './model/blockpairings-model-v2/model.json';  // TFJS saved model
const INPUT_CSV = './inference-data/predict_latea_30min_4minvar_pmpeak_oct-nov2025.csv';         // CSV containing rows to predict
const CATEGORY_MAP_FILE = './model/blockpairings-model-v2/category_maps.json';       // category maps saved from training
const NUMERIC_STATS_FILE = './model//blockpairings-model-v2/numeric_stats.json';     // mean/std saved from training
const CATEGORICAL_COLS = ['route_pair'];                     // columns that must be one-hot encoded
const THRESHOLD = 0.5;                                       // numeric output > threshold => class 1
const NUMERIC_COLUMNS = ['on_time_a', 'planned_layover_sec', 'avg_var_b', 'planned_dur_b', 'ampeak_a', 'pmpeak_a']; // numeric features

// -------------------
// LOAD CATEGORY MAPS & NUMERIC STATS
// -------------------
const categoryMaps = JSON.parse(fs.readFileSync(CATEGORY_MAP_FILE, 'utf8'));
const numericStats = JSON.parse(fs.readFileSync(NUMERIC_STATS_FILE, 'utf8'));

/**
 * ONE-HOT ENCODING
 */
function oneHotEncode(col, value) {
  const { map, size } = categoryMaps[col];
  const vec = Array(size).fill(0);
  if (map[value] !== undefined) vec[map[value]] = 1;  // unknown values stay all zeros
  return vec;
}

/**
 * LOAD CSV
 */
function loadCSV(path) {
  const content = fs.readFileSync(path, 'utf8');
  return parse(content, { columns: true, skip_empty_lines: true });
}

/**
 * ENCODE ROW
 */
function encodeRow(row) {
  const vector = [];

  // Numeric features (apply normalization using training mean/std)
  NUMERIC_COLUMNS.forEach(col => {
    const val = parseFloat(row[col]);
    const mean = numericStats.mean[col];
    const std = numericStats.std[col];
    vector.push((val - mean) / std);
  });

  // Categorical features
  CATEGORICAL_COLS.forEach(col => {
    vector.push(...oneHotEncode(col, row[col]));
  });

  return vector;
}

// -------------------
// PREDICTION SCRIPT
// -------------------
async function run() {
  console.log('Loading model...');
  const model = await tf.loadLayersModel(`file://${MODEL_PATH}`);

  console.log('Loading CSV...');
  const rows = loadCSV(INPUT_CSV);
  if (rows.length === 0) {
    console.error('CSV file is empty.');
    return;
  }

  console.log('Encoding rows...');
  const encoded = rows.map(row => encodeRow(row));

  console.log('Converting to tensor...');
  const inputTensor = tf.tensor2d(encoded);

  console.log('Running predictions...');
  const outputs = model.predict(inputTensor);
  const preds = outputs.arraySync();

  console.log('\n--- Predictions ---');
  const results = preds.map((p, i) => ({
    row: rows[i],
    prob: p[0],
    label: p[0] > THRESHOLD ? 1 : 0
  }));

  // Sort results by ascending probability
  results.sort((a, b) => a.prob - b.prob);

  console.log('\n--- Predictions (sorted by prob) ---');
  const colsToShow = ['route_pair'];

  results.forEach((r, i) => {
    const extra = colsToShow.map(c => `${c}=${r.row[c]}`).join(', ');
    console.log(`Row ${i + 1}: ${extra}, prob=${r.prob.toFixed(4)}, class=${r.label}`);
  });

  console.log('\nDone.');



  // --- Save predictions to file ---
  const outputRows = results.map(r => ({
    route_pair: r.row['route_pair'],
    prob: r.prob,
    class: r.label
  }));

  const outPath = './predictions/prediction_results.csv';

  const header = 'route_pair,prob,class\n';
  const lines = outputRows
    .map(r => `${r.route_pair},${r.prob.toFixed(6)},${r.class}`)
    .join('\n');

  fs.writeFileSync(outPath, header + lines, 'utf8');

  console.log(`\nPredictions saved to ${outPath}`);
}

run();
