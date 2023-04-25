import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// hospital
const totalHospitals = [];
let hospitals = [];
let rowCounter = 0;
let hospitalIdCounter = 1;
const importHospitals = () => { 
  parseFile('./exports/tbl_hospital.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const hospital = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_hospital'][key]) {
        if (key === 'Hosp_update') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            hospital[v1_v2_column_maps['tbl_hospital'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            hospital[v1_v2_column_maps['tbl_hospital'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            hospital[v1_v2_column_maps['tbl_hospital'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // hospital[v1_v2_column_maps['tbl_hospital'][key]] = format(new Date(date), 'yyyy-MM-dd');

            hospital[v1_v2_column_maps['tbl_hospital'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          hospital.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          hospital.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          hospital.worker_id = workerFINToId[row[key]];
        } else {
          hospital[v1_v2_column_maps['tbl_hospital'][key]] = row[key];
        }
      }
    });
    hospital.date_last_updated = today;
    hospital.created_by = 0;
    hospital.id = hospitalIdCounter++;

    if (hospital.worker_id && hospital.job_id && hospital.problem_id) {
      hospitals.push(hospital);
      rowCounter++;

      if (rowCounter === 100) {
        totalHospitals.push(hospitals);
        hospitals = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all hospitals
    totalHospitals.push(hospitals);
    const columns = Object.keys(totalHospitals[0][0]);

    for (let i = 0; i < totalHospitals.length; i += 1) {
      if (totalHospitals[i].length > 0) await postgreSQL`INSERT INTO public."hospital" ${postgreSQL(totalHospitals[i], columns)}`;
      console.log(`=== Inserted ${totalHospitals[i].length} hospitals ===`);
    }
  });
}

export {importHospitals};