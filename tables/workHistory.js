import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// workHistory
const totalWorkHistorys = [];
let workHistorys = [];
let rowCounter = 0;
let workHistoryIdCounter = 1;
const importWorkHistorys = () => { 
  parseFile('./exports/tbl_work_history.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const workHistory = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_workHistory'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            workHistory[v1_v2_column_maps['tbl_workHistory'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // workHistory[v1_v2_column_maps['tbl_workHistory'][key]] = date;

            workHistory[v1_v2_column_maps['tbl_workHistory'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          workHistory.job_id = jobKeyToId[row[key]];
        } else if (key === 'Work_hist_year_arrive') {
          if (row[key] !== "") workHistory[v1_v2_column_maps['tbl_workHistory'][key]] = parseInt(row[key]);
        } else if (key === 'Worker_FIN_number') {
          workHistory.worker_id = workerFINToId[row[key]];
        } else {
          workHistory[v1_v2_column_maps['tbl_workHistory'][key]] = row[key];
        }
      }
    });
    workHistory.date_last_updated = today;
    workHistory.created_by = 0;
    workHistory.id = workHistoryIdCounter++;

    if (workHistory.worker_id && workHistory.job_id) {
      workHistorys.push(workHistory);
      rowCounter++;

      if (rowCounter === 100) {
        totalWorkHistorys.push(workHistorys);
        workHistorys = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all workHistorys
    totalWorkHistorys.push(workHistorys);
    const columns = Object.keys(totalWorkHistorys[0][0]);

    for (let i = 0; i < totalWorkHistorys.length; i += 1) {
      await postgreSQL`INSERT INTO public."workHistory" ${postgreSQL(totalWorkHistorys[i], columns)}`;
      console.log(`=== Inserted ${totalWorkHistorys[i].length} workHistorys ===`);
    }
  });
}

export {importWorkHistorys};