import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// employer
const totalEmployers = [];
let employers = [];
let rowCounter = 0;
let employerIdCounter = 1;
const importEmployers = () => { 
  parseFile('./exports/tbl_employer.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const employer = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_employer'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            employer[v1_v2_column_maps['tbl_employer'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // employer[v1_v2_column_maps['tbl_employer'][key]] = date;

            employer[v1_v2_column_maps['tbl_employer'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          employer.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          employer.worker_id = workerFINToId[row[key]];
        } else {
          employer[v1_v2_column_maps['tbl_employer'][key]] = row[key];
        }
      }
    });
    employer.date_last_updated = today;
    employer.created_by = 0;
    employer.id = employerIdCounter++;

    if (employer.worker_id && employer.job_id) {
      employers.push(employer);
      rowCounter++;

      if (rowCounter === 100) {
        totalEmployers.push(employers);
        employers = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all employers
    totalEmployers.push(employers);
    const columns = Object.keys(totalEmployers[0][0]);

    for (let i = 0; i < totalEmployers.length; i += 1) {
      if (totalEmployers[i].length > 0) await postgreSQL`INSERT INTO public."employer" ${postgreSQL(totalEmployers[i], columns)}`;
      console.log(`=== Inserted ${totalEmployers[i].length} employers ===`);
    }
  });
}

export {importEmployers};