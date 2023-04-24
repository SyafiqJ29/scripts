import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// verbalAssurance
const totalVerbalAssurances = [];
let verbalAssurances = [];
let rowCounter = 0;
let verbalAssuranceIdCounter = 1;
const importVerbalAssurances = () => { 
  parseFile('./exports/tbl_verbal_assurances.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const verbalAssurance = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_verbalAssurances'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            verbalAssurance[v1_v2_column_maps['tbl_verbalAssurances'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // verbalAssurance[v1_v2_column_maps['tbl_verbalAssurances'][key]] = date;

            verbalAssurance[v1_v2_column_maps['tbl_verbalAssurances'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          verbalAssurance.job_id = jobKeyToId[row[key]];
        }  else if (key === 'Worker_FIN_number') {
          verbalAssurance.worker_id = workerFINToId[row[key]];
        } else {
          verbalAssurance[v1_v2_column_maps['tbl_verbalAssurances'][key]] = row[key];
        }
      }
    });
    verbalAssurance.date_last_updated = today;
    verbalAssurance.created_by = 0;
    verbalAssurance.id = verbalAssuranceIdCounter++;

    if (verbalAssurance.worker_id && verbalAssurance.job_id) {
      verbalAssurances.push(verbalAssurance);
      rowCounter++;

      if (rowCounter === 100) {
        totalVerbalAssurances.push(verbalAssurances);
        verbalAssurances = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all verbalAssurances
    totalVerbalAssurances.push(verbalAssurances);
    const columns = Object.keys(totalVerbalAssurances[0][0]);

    for (let i = 0; i < totalVerbalAssurances.length; i += 1) {
      if (totalVerbalAssurances[i].length > 0) await postgreSQL`INSERT INTO public."verbalAssurances" ${postgreSQL(totalVerbalAssurances[i], columns)}`;
      console.log(`=== Inserted ${totalVerbalAssurances[i].length} verbalAssurances ===`);
    }
  });
}

export {importVerbalAssurances};