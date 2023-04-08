import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// mcStatus
const totalMcStatuses = [];
let mcStatuses = [];
let rowCounter = 0;
let mcStatusIdCounter = 1;
const importMcStatuses = () => { 
  parseFile('./exports/tbl_MC_status.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const mcStatus = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_mcStatus'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            mcStatus[v1_v2_column_maps['tbl_mcStatus'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // mcStatus[v1_v2_column_maps['tbl_mcStatus'][key]] = date;

            mcStatus[v1_v2_column_maps['tbl_mcStatus'][key]] = row[key];
          }
        } else if (key === 'MC_update') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            mcStatus[v1_v2_column_maps['tbl_mcStatus'][key]] = '1920-01-01';
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            mcStatus[v1_v2_column_maps['tbl_mcStatus'][key]] = date;
          }
        } else if (key === 'MC_exp_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            mcStatus[v1_v2_column_maps['tbl_mcStatus'][key]] = null;
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            mcStatus[v1_v2_column_maps['tbl_mcStatus'][key]] = date;
          }
        } else if (key === 'Prob_key') {
          mcStatus.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          mcStatus.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          mcStatus.worker_id = workerFINToId[row[key]];
        } else {
          mcStatus[v1_v2_column_maps['tbl_mcStatus'][key]] = row[key];
        }
      }
    });
    mcStatus.date_last_updated = today;
    mcStatus.created_by = 0;
    mcStatus.id = mcStatusIdCounter++;

    if (mcStatus.worker_id && mcStatus.job_id && mcStatus.problem_id) {
      mcStatuses.push(mcStatus);
      rowCounter++;

      if (rowCounter === 100) {
        totalMcStatuses.push(mcStatuses);
        mcStatuses = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all mcStatuses
    totalMcStatuses.push(mcStatuses);
    const columns = Object.keys(totalMcStatuses[0][0]);

    for (let i = 0; i < totalMcStatuses.length; i += 1) {
      await postgreSQL`INSERT INTO public."mcStatus" ${postgreSQL(totalMcStatuses[i], columns)}`;
      console.log(`=== Inserted ${totalMcStatuses[i].length} mcStatuses ===`);
    }
  });
}

export {importMcStatuses};