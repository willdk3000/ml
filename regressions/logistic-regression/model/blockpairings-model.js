import * as tf from '@tensorflow/tfjs-node';
import fs from 'fs';
import { parse } from 'csv-parse/sync';  // use /sync subpath officially supported

// -------------------
// CONFIGURATION
// -------------------
const csvFile = '../../data/trips_nov2025.csv';
const numericColumns = ['on_time_a', 'planned_layover_sec', 'ampeak_a', 'pmpeak_a']; // replace with your columns
const categoricalColumns = ['route_pair']
const labelColumn = 'y_on_time_b'; // replace with your binary label column

// -------------------
// LOAD CSV
// -------------------
const rawCSV = fs.readFileSync(csvFile, 'utf8');
const records = parse(rawCSV, { columns: true, skip_empty_lines: true });

// -------------------
// BUILD CATEGORY MAPPINGS
// -------------------

//Get all categoricalColumns
const categoryMaps = {};
for (const col of categoricalColumns) {
    const uniqueValues = [...new Set(records.map(r => r[col]))];
    const map = {};
    uniqueValues.forEach((v, i) => { map[v] = i; });
    categoryMaps[col] = { map, size: uniqueValues.length };
}

//Encode oneHot categorical ex.: from 8_54 to arrays like [0,0,1]
function oneHotEncode(col, value) {
    const { map, size } = categoryMaps[col];
    const vec = Array(size).fill(0);
    vec[map[value]] = 1;
    return vec;
}

fs.writeFileSync('category_maps.json', JSON.stringify(categoryMaps));

// -------------------
// PREPARE FEATURES AND LABELS
// -------------------
let features = [];
let labels = [];

for (const row of records) {
    const numericValues = numericColumns.map(c => parseFloat(row[c]));
    if (numericValues.some(Number.isNaN)) continue;

    let rowFeatures = [...numericValues];
    for (const col of categoricalColumns) {
        rowFeatures = rowFeatures.concat(oneHotEncode(col, row[col]));
    }

    features.push(rowFeatures);
    labels.push([parseFloat(row[labelColumn])]);
}

if (features.length === 0) throw new Error('No valid rows found');

//Prepare tensors for calculations
const X = tf.tensor2d(features, [features.length, features[0].length]);
const y = tf.tensor2d(labels, [labels.length, 1]);

// -------------------
// NORMALIZE NUMERIC FEATURES
// -------------------
const numericX = X.slice([0, 0], [-1, numericColumns.length]);
const { mean, variance } = tf.moments(numericX, 0);
const std = variance.sqrt();
const numericXNorm = numericX.sub(mean).div(std);

// Concatenate normalized numeric features with categorical one-hot features
let X_normalized = numericXNorm;
if (categoricalColumns.length > 0) {
    const categoricalX = X.slice([0, numericColumns.length], [-1, -1]);
    X_normalized = tf.concat([numericXNorm, categoricalX], 1);
}

// Export to file for reuse
// numericColumns is your array of numeric feature names
const meanObj = {};
const stdObj = {};
numericColumns.forEach((col, i) => {
    meanObj[col] = mean.arraySync()[i];  // value of column i
    stdObj[col] = std.arraySync()[i];    // std of column i
});

fs.writeFileSync('./numeric_stats.json', JSON.stringify({
    mean: meanObj,
    std: stdObj
}));

console.log('Numeric stats saved to numeric_stats.json');


// -------------------
// BUILD MODEL
// -------------------
const model = tf.sequential();
model.add(tf.layers.dense({
    units: 1,
    inputShape: [X_normalized.shape[1]],
    activation: 'sigmoid'
}));
model.compile({ optimizer: tf.train.adam(0.001), loss: 'binaryCrossentropy', metrics: ['accuracy'] });

// -------------------
// TRAIN MODEL
// -------------------
(async () => {
    await model.fit(X_normalized, y, {
        //iterations = epochs
        epochs: 20,
        //batch size = number of samples the gradient is calculated on before moving
        //on to the next iteration
        batchSize: 32,
        //validationSplit is the % of the sample data used for training
        validationSplit: 0.2,
        shuffle: true,
        callbacks: { onEpochEnd: (epoch, logs) => console.log(`Epoch ${epoch + 1}: loss=${logs.loss.toFixed(4)}, acc=${logs.acc.toFixed(4)}`) }
    });


    // Save the trained model for later use
    await model.save('file://./blockpairings-model');
    console.log('Model saved to ./blockpairings-model/');


    // // -------------------
    // // PREDICT MULTIPLE SAMPLES
    // // -------------------
    // const testNumeric = [
    //     [1, 300, 1, 0],
    //     [0, 300, 1, 0],
    //     [1, 600, 1, 0],
    //     [0, 600, 1, 0],
    //     [1, 300, 0, 0],
    //     [0, 300, 0, 0],
    //     [1, 600, 0, 0],
    //     [0, 600, 0, 0],
    //     [1, 300, 1, 0],
    //     [0, 300, 1, 0],
    //     [1, 600, 1, 0],
    //     [0, 600, 1, 0],
    //     [1, 300, 0, 0],
    //     [0, 300, 0, 0],
    //     [1, 600, 0, 0],
    //     [0, 600, 0, 0],
    // ];

    // const testCategoricalValues = [
    //     ['8_73'],
    //     ['8_73'],
    //     ['8_73'],
    //     ['8_73'],
    //     ['8_73'],
    //     ['8_73'],
    //     ['8_73'],
    //     ['8_73'],
    //     ['6_54'],
    //     ['6_54'],
    //     ['6_54'],
    //     ['6_54'],
    //     ['6_54'],
    //     ['6_54'],
    //     ['6_54'],
    //     ['6_54'],
    // ];

    // const testNumericTensor = tf.tensor2d(testNumeric).sub(mean).div(std);
    // const testCategoricalTensor = tf.tensor2d(
    //     testCategoricalValues.map(vals => {
    //         let encoded = [];
    //         categoricalColumns.forEach((col, idx) => {
    //             encoded = encoded.concat(oneHotEncode(col, vals[idx]));
    //         });
    //         return encoded;
    //     })
    // );

    // const testInput = tf.concat([testNumericTensor, testCategoricalTensor], 1);
    // const predictions = model.predict(testInput);
    // predictions.print();


})();