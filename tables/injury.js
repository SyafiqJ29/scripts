import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// injury
const totalInjurys = [];
let injurys = [];
let rowCounter = 0;
let injuryIdCounter = 1;
const importInjurys = () => { 
  parseFile('./exports/tbl_injury.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const injury = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_injury'][key]) {
        if (key === 'Injury_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            injury[v1_v2_column_maps['tbl_injury'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            injury[v1_v2_column_maps['tbl_injury'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            injury[v1_v2_column_maps['tbl_injury'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // injury[v1_v2_column_maps['tbl_injury'][key]] = format(new Date(date), 'yyyy-MM-dd');

            injury[v1_v2_column_maps['tbl_injury'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          injury.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          injury.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          injury.worker_id = workerFINToId[row[key]];
        } else {
          injury[v1_v2_column_maps['tbl_injury'][key]] = row[key];
        }
      }
    });
    injury.date_last_updated = today;
    injury.created_by = 0;
    injury.id = injuryIdCounter++;

    if (injury.worker_id && injury.job_id && injury.problem_id) {
      injurys.push(injury);
      rowCounter++;

      if (rowCounter === 100) {
        totalInjurys.push(injurys);
        injurys = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all injurys
    totalInjurys.push(injurys);
    const columns = Object.keys(totalInjurys[0][0]);

    for (let i = 0; i < totalInjurys.length; i += 1) {
      if (totalInjurys[i].length > 0) await postgreSQL`INSERT INTO public."injury" ${postgreSQL(totalInjurys[i], columns)}`;
      console.log(`=== Inserted ${totalInjurys[i].length} injurys ===`);
    }
  });
}

export {importInjurys};