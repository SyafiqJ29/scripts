import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// lawyer
const totalLawyers = [];
let lawyers = [];
let rowCounter = 0;
let lawyerIdCounter = 1;
const importLawyers = () => { 
  parseFile('./exports/tbl_lawyer.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const lawyer = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_lawyer'][key]) {
        if (key === 'Lawyer_update') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            lawyer[v1_v2_column_maps['tbl_lawyer'][key]] = '1920-01-01';
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            lawyer[v1_v2_column_maps['tbl_lawyer'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            lawyer[v1_v2_column_maps['tbl_lawyer'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // lawyer[v1_v2_column_maps['tbl_lawyer'][key]] = date;

            lawyer[v1_v2_column_maps['tbl_lawyer'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          lawyer.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          lawyer.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          lawyer.worker_id = workerFINToId[row[key]];
        } else {
          lawyer[v1_v2_column_maps['tbl_lawyer'][key]] = row[key];
        }
      }
    });
    lawyer.date_last_updated = today;
    lawyer.created_by = 0;
    lawyer.id = lawyerIdCounter++;

    if (lawyer.worker_id && lawyer.job_id && lawyer.problem_id) {
      lawyers.push(lawyer);
      rowCounter++;

      if (rowCounter === 100) {
        totalLawyers.push(lawyers);
        lawyers = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all lawyers
    totalLawyers.push(lawyers);
    const columns = Object.keys(totalLawyers[0][0]);

    for (let i = 0; i < totalLawyers.length; i += 1) {
      if (totalLawyers[i].length > 0) await postgreSQL`INSERT INTO public."lawyer" ${postgreSQL(totalLawyers[i], columns)}`;
      console.log(`=== Inserted ${totalLawyers[i].length} lawyers ===`);
    }
  });
}

export {importLawyers};