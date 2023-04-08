import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// employmentContract
const totalEmploymentContracts = [];
let employmentContracts = [];
let rowCounter = 0;
let employmentContractIdCounter = 1;
const importEmploymentContracts = () => { 
  parseFile('./exports/tbl_employment_contract.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const employmentContract = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_employmentContract'][key]) {
        if (key === 'Contract_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            employmentContract[v1_v2_column_maps['tbl_employmentContract'][key]] = '1920-01-01';
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            employmentContract[v1_v2_column_maps['tbl_employmentContract'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            employmentContract[v1_v2_column_maps['tbl_employmentContract'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // employmentContract[v1_v2_column_maps['tbl_employmentContract'][key]] = date;

            employmentContract[v1_v2_column_maps['tbl_employmentContract'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          employmentContract.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_Number') {
          employmentContract.worker_id = workerFINToId[row[key]];
        } else {
          employmentContract[v1_v2_column_maps['tbl_employmentContract'][key]] = row[key];
        }
      }
    });
    employmentContract.date_last_updated = today;
    employmentContract.created_by = 0;
    employmentContract.id = employmentContractIdCounter++;

    if (employmentContract.worker_id && employmentContract.job_id) {
      employmentContracts.push(employmentContract);
      rowCounter++;

      if (rowCounter === 100) {
        totalEmploymentContracts.push(employmentContracts);
        employmentContracts = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all employmentContracts
    totalEmploymentContracts.push(employmentContracts);
    const columns = Object.keys(totalEmploymentContracts[0][0]);

    for (let i = 0; i < totalEmploymentContracts.length; i += 1) {
      await postgreSQL`INSERT INTO public."employmentContract" ${postgreSQL(totalEmploymentContracts[i], columns)}`;
      console.log(`=== Inserted ${totalEmploymentContracts[i].length} employmentContracts ===`);
    }
  });
}

export {importEmploymentContracts};