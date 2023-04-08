import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// salaryHistory
const totalSalaryHistorys = [];
let salaryHistorys = [];
let rowCounter = 0;
let salaryHistoryIdCounter = 1;
const importSalaryHistorys = () => { 
  parseFile('./exports/tbl_lead_case_worker.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const salaryHistory = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_salaryHistory'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            salaryHistory[v1_v2_column_maps['tbl_salaryHistory'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // salaryHistory[v1_v2_column_maps['tbl_salaryHistory'][key]] = date;

            salaryHistory[v1_v2_column_maps['tbl_salaryHistory'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          salaryHistory.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          salaryHistory.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          salaryHistory.worker_id = workerFINToId[row[key]];
        } else {
          salaryHistory[v1_v2_column_maps['tbl_salaryHistory'][key]] = row[key];
        }
      }
    });
    salaryHistory.date_last_updated = today;
    salaryHistory.created_by = 0;
    salaryHistory.sal_whether_get_ipa = 'no info at migration';
    salaryHistory.id = salaryHistoryIdCounter++;

    if (salaryHistory.worker_id && salaryHistory.job_id && salaryHistory.problem_id) {
      salaryHistorys.push(salaryHistory);
      rowCounter++;

      if (rowCounter === 100) {
        totalSalaryHistorys.push(salaryHistorys);
        salaryHistorys = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all salaryHistorys
    totalSalaryHistorys.push(salaryHistorys);
    const columns = Object.keys(totalSalaryHistorys[0][0]);

    for (let i = 0; i < totalSalaryHistorys.length; i += 1) {
      await postgreSQL`INSERT INTO public."salaryHistory" ${postgreSQL(totalSalaryHistorys[i], columns)}`;
      console.log(`=== Inserted ${totalSalaryHistorys[i].length} salaryHistorys ===`);
    }
  });
}

export {importSalaryHistorys};