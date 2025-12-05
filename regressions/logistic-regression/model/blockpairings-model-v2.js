import * as tf from '@tensorflow/tfjs-node';
import fs from 'fs';
import { parse } from 'csv-parse/sync';

// -------------------
// V2
// -------------------

// This version can handle millions of rows

// -------------------
// CONFIGURATION
// -------------------
const csvFile = '../../data-samples/trips_oct_nov2025.csv';
const numericColumns = ['on_time_a', 'planned_layover_sec', 'planned_dur_a', 'planned_dur_b', 'ampeak_a', 'pmpeak_a'];
const categoricalColumns = ['route_pair'];
const labelColumn = 'y_on_time_b';
const BATCH_SIZE = 1024;
const EPOCHS = 20;

// -------------------
// LOAD CSV
// -------------------
const rawCSV = fs.readFileSync(csvFile, 'utf8');
const records = parse(rawCSV, { columns: true, skip_empty_lines: true });

// -------------------
// BUILD CATEGORY MAPPINGS
// -------------------
const categoryMaps = {};
for (const col of categoricalColumns) {
  const uniqueValues = [...new Set(records.map(r => r[col]))];
  const map = {};
  uniqueValues.forEach((v, i) => { map[v] = i; });
  categoryMaps[col] = { map, size: uniqueValues.length };
}
fs.writeFileSync('category_maps.json', JSON.stringify(categoryMaps));

function oneHotEncode(col, value) {
  const { map, size } = categoryMaps[col];
  const vec = Array(size).fill(0);
  vec[map[value]] = 1;
  return vec;
}

// -------------------
// CALCULATE NUMERIC MEAN & STD
// -------------------
const numericData = records.map(r => numericColumns.map(c => parseFloat(r[c])).filter(v => isFinite(v)));
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
// CREATE DATASET GENERATOR
// -------------------
function* dataGenerator() {
  for (const row of records) {
    const numericValues = numericColumns.map(c => parseFloat(row[c]));
    if (numericValues.some(v => !isFinite(v))) continue;

    // Normalize numeric
    const numericNorm = numericValues.map((v, i) => (v - meanObj[numericColumns[i]]) / stdObj[numericColumns[i]]);

    // One-hot encode categorical
    let catValues = [];
    for (const col of categoricalColumns) {
      catValues = catValues.concat(oneHotEncode(col, row[col]));
    }

    const xs = numericNorm.concat(catValues);
    const ys = [parseFloat(row[labelColumn])];
    yield { xs, ys };
  }
}

// -------------------
// CREATE TF.DATA DATASET
// -------------------
const dataset = tf.data.generator(dataGenerator).batch(BATCH_SIZE);

// -------------------
// BUILD MODEL
// -------------------
const inputDim = numericColumns.length + categoricalColumns.reduce((sum, c) => sum + categoryMaps[c].size, 0);
const model = tf.sequential();
model.add(tf.layers.dense({ units: 1, inputShape: [inputDim], activation: 'sigmoid' }));

//0.001 is learning rate using Adam
model.compile({
  optimizer: tf.train.adam(0.001),
  loss: 'binaryCrossentropy',
  metrics: ['accuracy']
});

// Using SGD with a learning rate of 0.01
// model.compile({
//     optimizer: tf.train.sgd(0.01),  // <-- SGD optimizer
//     loss: 'binaryCrossentropy',
//     metrics: ['accuracy']
// });



// -------------------
// TRAIN MODEL
// -------------------
(async () => {
  await model.fitDataset(dataset, {
    epochs: EPOCHS,
    callbacks: {
      onEpochEnd: (epoch, logs) =>
        console.log(`Epoch ${epoch + 1}: loss=${logs.loss.toFixed(4)}, acc=${logs.acc.toFixed(4)}`)
    }
  });

  await model.save('file://./blockpairings-model-v2');
  console.log('Model saved to ./blockpairings-model-v2/');
})();
