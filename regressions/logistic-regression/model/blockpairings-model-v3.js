import * as tf from '@tensorflow/tfjs-node';
import fs from 'fs';
import { parse } from 'csv-parse/sync';

// -------------------
// CONFIGURATION
// -------------------
const csvFile = '../../data-samples/trips_oct_nov2025_lstm.csv';
const numericColumns = ['planned_layover', 'plannedduration', 'p85_pct_b', 'range7525_b', 'route_id', 'direction_id', 'ampeak_a', 'pmpeak_a'];
const categoricalColumns = []; // leave empty [] if no categorical features
const labelColumn = 'on_time_class';
const MAX_TIMESTEPS = 25; // max trips per block
const BATCH_SIZE = 1024;
const EPOCHS = 20;

// -------------------
// LOAD CSV
// -------------------
const rawCSV = fs.readFileSync(csvFile, 'utf8');
const records = parse(rawCSV, { columns: true, skip_empty_lines: true });

// -------------------
// BUILD CATEGORY MAPS (if any)
// -------------------
const categoryMaps = {};
if (categoricalColumns.length > 0) {
  for (const col of categoricalColumns) {
    const uniqueValues = [...new Set(records.map(r => r[col]))];
    const map = {};
    uniqueValues.forEach((v, i) => { map[v] = i; });
    categoryMaps[col] = { map, size: uniqueValues.length };
  }
  fs.writeFileSync('./category_maps.json', JSON.stringify(categoryMaps));
}

// -------------------
// ONE-HOT ENCODE FUNCTION
// -------------------
function oneHotEncode(col, value) {
  if (!categoryMaps[col]) return [];
  const { map, size } = categoryMaps[col];
  const vec = Array(size).fill(0);
  if (map[value] !== undefined) vec[map[value]] = 1;
  return vec;
}

// -------------------
// CALCULATE NUMERIC MEAN & STD
// -------------------
const numericData = records.map(r =>
  numericColumns.map(c => parseFloat(r[c])).filter(v => isFinite(v))
);
const numericTensor = tf.tensor2d(numericData);
const { mean, variance } = tf.moments(numericTensor, 0);
const std = variance.sqrt();

const meanObj = {};
const stdObj = {};
numericColumns.forEach((col, i) => {
  meanObj[col] = mean.arraySync()[i];
  stdObj[col] = std.arraySync()[i];
});
fs.writeFileSync('./numeric_stats.json', JSON.stringify({ mean: meanObj, std: stdObj }));

numericTensor.dispose();
variance.dispose();
std.dispose();

// -------------------
// CREATE SEQUENCE GENERATOR
// -------------------
function* sequenceGenerator() {
  // group trips by block
  const blocks = {};
  for (const row of records) {
    const block = row.block_key;
    if (!blocks[block]) blocks[block] = [];
    blocks[block].push(row);
  }

  for (const blockKey in blocks) {
    const trips = blocks[blockKey].sort((a, b) => a.trip - b.trip);
    const xsSeq = [];
    const ysSeq = [];

    for (let i = 0; i < MAX_TIMESTEPS; i++) {
      if (i < trips.length) {
        const row = trips[i];
        const numericValues = numericColumns.map(c => parseFloat(row[c]));
        const numericNorm = numericValues.map((v, j) => (v - meanObj[numericColumns[j]]) / stdObj[numericColumns[j]]);
        let catValues = [];
        for (const col of categoricalColumns) {
          catValues = catValues.concat(oneHotEncode(col, row[col]));
        }
        xsSeq.push(numericNorm.concat(catValues));
        ysSeq.push([parseFloat(row[labelColumn])]);
      } else {
        const inputDim = numericColumns.length + categoricalColumns.reduce((sum, c) => sum + (categoryMaps[c]?.size || 0), 0);
        xsSeq.push(new Array(inputDim).fill(0));
        ysSeq.push([0]);
      }
    }

    // yield tensors instead of arrays
    yield { 
      xs: tf.tensor2d(xsSeq),    // shape [25, features]
      ys: tf.tensor2d(ysSeq)     // shape [25, 1]
    };
  }
}


// -------------------
// CREATE TF.DATA DATASET
// -------------------
const inputDim = numericColumns.length + categoricalColumns.reduce((sum, c) => sum + (categoryMaps[c]?.size || 0), 0);

const dataset = tf.data
  .generator(sequenceGenerator)
  .shuffle(20000)
  .batch(BATCH_SIZE);

// -------------------
// BUILD LSTM MODEL
// -------------------
const model = tf.sequential();

// Mask timesteps with all zeros
model.add(tf.layers.masking({ maskValue: 0, inputShape: [MAX_TIMESTEPS, inputDim] }));

// LSTM
model.add(tf.layers.lstm({
  units: 25,
  activation: 'tanh',
  returnSequences: true
}));

// Output per timestep
model.add(tf.layers.dense({
  units: 1,
  activation: 'sigmoid'
}));

model.compile({
  optimizer: tf.train.adam(0.001),
  loss: 'binaryCrossentropy',
  metrics: ['accuracy']
});

// -------------------
// TRAIN MODEL
// -------------------
(async () => {
  await model.fitDataset(dataset, {
    epochs: EPOCHS,
    callbacks: {
      onEpochEnd: (epoch, logs) => console.log(`Epoch ${epoch + 1}: loss=${logs.loss.toFixed(4)}, acc=${logs.acc.toFixed(4)}`)
    }
  });

  await model.save('file://./blockpairings-model-v3');
  console.log('Model saved to ./blockpairings-model-v3/');
})();
