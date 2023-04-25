import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// leadCaseWorker
const totalLeadCaseWorkers = [];
let leadCaseWorkers = [];
let rowCounter = 0;
let leadCaseWorkerIdCounter = 1;
const importLeadCaseWorkers = () => { 
  parseFile('./exports/tbl_lead_case_worker.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const leadCaseWorker = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_leadCaseWorker'][key]) {
        if (key === 'Lead_start') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            leadCaseWorker[v1_v2_column_maps['tbl_leadCaseWorker'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            leadCaseWorker[v1_v2_column_maps['tbl_leadCaseWorker'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');
          }
        } else if (key === 'Lead_end') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            leadCaseWorker[v1_v2_column_maps['tbl_leadCaseWorker'][key]] = null;
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            leadCaseWorker[v1_v2_column_maps['tbl_leadCaseWorker'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            leadCaseWorker[v1_v2_column_maps['tbl_leadCaseWorker'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // leadCaseWorker[v1_v2_column_maps['tbl_leadCaseWorker'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            leadCaseWorker[v1_v2_column_maps['tbl_leadCaseWorker'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          leadCaseWorker.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          leadCaseWorker.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          leadCaseWorker.worker_id = workerFINToId[row[key]];
        } else {
          leadCaseWorker[v1_v2_column_maps['tbl_leadCaseWorker'][key]] = row[key];
        }
      }
    });
    leadCaseWorker.date_last_updated = today;
    leadCaseWorker.created_by = 0;
    leadCaseWorker.id = leadCaseWorkerIdCounter++;

    if (leadCaseWorker.worker_id && leadCaseWorker.job_id && leadCaseWorker.problem_id) {
      leadCaseWorkers.push(leadCaseWorker);
      rowCounter++;

      if (rowCounter === 100) {
        totalLeadCaseWorkers.push(leadCaseWorkers);
        leadCaseWorkers = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all leadCaseWorkers
    totalLeadCaseWorkers.push(leadCaseWorkers);
    const columns = Object.keys(totalLeadCaseWorkers[0][0]);

    for (let i = 0; i < totalLeadCaseWorkers.length; i += 1) {
      if (totalLeadCaseWorkers[i].length > 0) await postgreSQL`INSERT INTO public."leadCaseWorker" ${postgreSQL(totalLeadCaseWorkers[i], columns)}`;
      console.log(`=== Inserted ${totalLeadCaseWorkers[i].length} leadCaseWorkers ===`);
    }
  });
}

export {importLeadCaseWorkers};