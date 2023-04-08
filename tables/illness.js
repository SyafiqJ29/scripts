import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// illness
const totalIllnesss = [];
let illnesss = [];
let rowCounter = 0;
let illnessIdCounter = 1;
const importIllnesss = () => { 
  parseFile('./exports/tbl_illness.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const illness = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_illness'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            illness[v1_v2_column_maps['tbl_illness'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // illness[v1_v2_column_maps['tbl_illness'][key]] = date;

            illness[v1_v2_column_maps['tbl_illness'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          illness.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          illness.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          illness.worker_id = workerFINToId[row[key]];
        } else {
          illness[v1_v2_column_maps['tbl_illness'][key]] = row[key];
        }
      }
    });
    illness.date_last_updated = today;
    illness.created_by = 0;
    illness.id = illnessIdCounter++;

    if (illness.worker_id && illness.job_id && illness.problem_id) {
      illnesss.push(illness);
      rowCounter++;

      if (rowCounter === 100) {
        totalIllnesss.push(illnesss);
        illnesss = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all illnesss
    totalIllnesss.push(illnesss);
    const columns = Object.keys(totalIllnesss[0][0]);

    for (let i = 0; i < totalIllnesss.length; i += 1) {
      await postgreSQL`INSERT INTO public."illness" ${postgreSQL(totalIllnesss[i], columns)}`;
      console.log(`=== Inserted ${totalIllnesss[i].length} illnesss ===`);
    }
  });
}

export {importIllnesss};