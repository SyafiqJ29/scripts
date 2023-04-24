import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// currentPass
const totalCurrentPasses = [];
let currentPasses = [];
let rowCounter = 0;
let currentPassIdCounter = 1;
const importCurrentPasses = () => { 
  parseFile('./exports/tbl_pass_details.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const currentPass = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_currentPass'][key]) {
        if (key === 'Pass_application_date' || key === 'Pass_issue_date' || key === 'Pass_expiry_date' || key === 'Pass_obsolete_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            currentPass[v1_v2_column_maps['tbl_currentPass'][key]] = null;
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            currentPass[v1_v2_column_maps['tbl_currentPass'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            currentPass[v1_v2_column_maps['tbl_currentPass'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // currentPass[v1_v2_column_maps['tbl_currentPass'][key]] = date;

            currentPass[v1_v2_column_maps['tbl_currentPass'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          currentPass.job_id = jobKeyToId[row[key]];
        }  else if (key === 'Worker_FIN_number') {
          currentPass.worker_id = workerFINToId[row[key]];
        } else {
          currentPass[v1_v2_column_maps['tbl_currentPass'][key]] = row[key];
        }
      }
    });
    currentPass.date_last_updated = today;
    currentPass.created_by = 0;
    currentPass.id = currentPassIdCounter++;

    if (currentPass.worker_id && currentPass.job_id) {
      currentPasses.push(currentPass);
      rowCounter++;

      if (rowCounter === 100) {
        totalCurrentPasses.push(currentPasses);
        currentPasses = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all currentPasses
    totalCurrentPasses.push(currentPasses);
    const columns = Object.keys(totalCurrentPasses[0][0]);

    for (let i = 0; i < totalCurrentPasses.length; i += 1) {
      if (totalCurrentPasses[i].length > 0) await postgreSQL`INSERT INTO public."currentPass" ${postgreSQL(totalCurrentPasses[i], columns)}`;
      console.log(`=== Inserted ${totalCurrentPasses[i].length} currentPasses ===`);
    }
  });
}

export {importCurrentPasses};