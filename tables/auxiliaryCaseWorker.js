import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// auxiliaryCaseWorker
const totalAuxiliaryCaseWorkers = [];
let auxiliaryCaseWorkers = [];
let rowCounter = 0;
let auxiliaryCaseWorkerIdCounter = 1;
const importAuxiliaryCaseWorkers = () => { 
  parseFile('./exports/tbl_auxillarycaseworker.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const auxiliaryCaseWorker = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]) {
        if (key === 'Aux_start') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            auxiliaryCaseWorker[v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            auxiliaryCaseWorker[v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Aux_end') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            auxiliaryCaseWorker[v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]] = null;
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            auxiliaryCaseWorker[v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            auxiliaryCaseWorker[v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // auxiliaryCaseWorker[v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]] = format(new Date(date), 'yyyy-MM-dd');

            auxiliaryCaseWorker[v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          auxiliaryCaseWorker.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          auxiliaryCaseWorker.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          auxiliaryCaseWorker.worker_id = workerFINToId[row[key]];
        } else {
          auxiliaryCaseWorker[v1_v2_column_maps['tbl_auxiliaryCaseWorker'][key]] = row[key];
        }
      }
    });
    auxiliaryCaseWorker.date_last_updated = today;
    auxiliaryCaseWorker.created_by = 0;
    auxiliaryCaseWorker.id = auxiliaryCaseWorkerIdCounter++;

    if (auxiliaryCaseWorker.worker_id && auxiliaryCaseWorker.job_id && auxiliaryCaseWorker.problem_id) {
      auxiliaryCaseWorkers.push(auxiliaryCaseWorker);
      rowCounter++;

      if (rowCounter === 100) {
        totalAuxiliaryCaseWorkers.push(auxiliaryCaseWorkers);
        auxiliaryCaseWorkers = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all auxiliaryCaseWorkers
    totalAuxiliaryCaseWorkers.push(auxiliaryCaseWorkers);
    const columns = Object.keys(totalAuxiliaryCaseWorkers[0][0]);

    for (let i = 0; i < totalAuxiliaryCaseWorkers.length; i += 1) {
      if (totalAuxiliaryCaseWorkers[i].length > 0) await postgreSQL`INSERT INTO public."auxiliaryCaseWorker" ${postgreSQL(totalAuxiliaryCaseWorkers[i], columns)}`;
      console.log(`=== Inserted ${totalAuxiliaryCaseWorkers[i].length} auxiliaryCaseWorkers ===`);
    }
  });
}

export {importAuxiliaryCaseWorkers};