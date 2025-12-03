import fs from 'fs';
import { parse } from 'csv-parse/sync';

/**
 * INPUT FILES
 * 1) Sample data containing route_pair (you want to analyze these)
 * 2) Prediction output file created earlier
 */
const SAMPLE_CSV = '../data-samples/trips_nov2025.csv';
const PREDICTIONS_CSV = './predictions/prediction_results.csv';


// Load a CSV as array of objects
function loadCSV(path) {
    const content = fs.readFileSync(path, 'utf8');
    return parse(content, { columns: true, skip_empty_lines: true });
}


// --- MAIN ---
function run() {
    console.log('Loading sample data...');
    const sample = loadCSV(SAMPLE_CSV);

    console.log('Loading prediction results...');
    const predictions = loadCSV(PREDICTIONS_CSV);

    // Step 1: Count occurrences of each route_pair in sample data
    const counts = {};   // route_pair → count

    for (const row of sample) {
        const rp = row['route_pair'];
        if (!counts[rp]) counts[rp] = 0;
        counts[rp]++;
    }

    // Step 2: Merge counts with predictions
    const joined = predictions.map(pred => {
        const rp = pred.route_pair;

        return {
            route_pair: rp,
            prob: parseFloat(pred.prob),
            class: parseInt(pred.class, 10),
            count: counts[rp] || 0
        };
    });

    // Step 3: Sort by route_pair or probability (your choice)
    //joined.sort((a, b) => a.route_pair.localeCompare(b.route_pair));
    //joined.sort((a, b) => b.count - a.count);
    joined.sort((a, b) => a.prob - b.prob);

    // Step 4: Save final merged results
    const outPath = './predictions/routepair_summary.csv';
    const header = 'route_pair,prob,class,count\n';
    const lines = joined
        .map(r => `${r.route_pair},${r.prob.toFixed(6)},${r.class},${r.count}`)
        .join('\n');

    fs.writeFileSync(outPath, header + lines);

    console.log(`Done. Output saved to ${outPath}`);
}

run();
