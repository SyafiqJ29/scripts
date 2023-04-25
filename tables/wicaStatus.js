import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// wicaStatus
const totalWicaStatuses = [];
let wicaStatuses = [];
let rowCounter = 0;
let wicaStatusIdCounter = 1;
const importWicaStatuses = () => { 
  parseFile('./exports/tbl_wica.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const wicaStatus = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_wicaStatus'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            wicaStatus[v1_v2_column_maps['tbl_wicaStatus'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // wicaStatus[v1_v2_column_maps['tbl_wicaStatus'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            wicaStatus[v1_v2_column_maps['tbl_wicaStatus'][key]] = row[key];
          }
        } else if (key === 'Wicamon_update') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            wicaStatus[v1_v2_column_maps['tbl_wicaStatus'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            wicaStatus[v1_v2_column_maps['tbl_wicaStatus'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');
          }
        } else if (key === 'Prob_key') {
          wicaStatus.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          wicaStatus.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          wicaStatus.worker_id = workerFINToId[row[key]];
        } else {
          wicaStatus[v1_v2_column_maps['tbl_wicaStatus'][key]] = row[key];
        }
      }
    });
    wicaStatus.date_last_updated = today;
    wicaStatus.created_by = 0;
    wicaStatus.id = wicaStatusIdCounter++;

    if (wicaStatus.worker_id && wicaStatus.job_id && wicaStatus.problem_id) {
      wicaStatuses.push(wicaStatus);
      rowCounter++;

      if (rowCounter === 100) {
        totalWicaStatuses.push(wicaStatuses);
        wicaStatuses = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all wicaStatuses
    totalWicaStatuses.push(wicaStatuses);
    const columns = Object.keys(totalWicaStatuses[0][0]);

    for (let i = 0; i < totalWicaStatuses.length; i += 1) {
      if (totalWicaStatuses[i].length > 0) await postgreSQL`INSERT INTO public."wicaStatus" ${postgreSQL(totalWicaStatuses[i], columns)}`;
      console.log(`=== Inserted ${totalWicaStatuses[i].length} wicaStatuses ===`);
    }
  });
}

export {importWicaStatuses};