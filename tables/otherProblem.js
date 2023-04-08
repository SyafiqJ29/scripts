import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// otherProblem
const totalOtherProblems = [];
let otherProblems = [];
let rowCounter = 0;
let otherProblemIdCounter = 1;
const importOtherProblems = () => { 
  parseFile('./exports/tbl_other_problems.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const otherProblem = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_otherProblem'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            otherProblem[v1_v2_column_maps['tbl_otherProblem'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // otherProblem[v1_v2_column_maps['tbl_otherProblem'][key]] = date;

            otherProblem[v1_v2_column_maps['tbl_otherProblem'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          otherProblem.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          otherProblem.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          otherProblem.worker_id = workerFINToId[row[key]];
        } else {
          otherProblem[v1_v2_column_maps['tbl_otherProblem'][key]] = row[key];
        }
      }
    });
    otherProblem.date_last_updated = today;
    otherProblem.created_by = 0;
    otherProblem.id = otherProblemIdCounter++;

    if (otherProblem.worker_id && otherProblem.job_id && otherProblem.problem_id) {
      otherProblems.push(otherProblem);
      rowCounter++;

      if (rowCounter === 100) {
        totalOtherProblems.push(otherProblems);
        otherProblems = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all otherProblems
    totalOtherProblems.push(otherProblems);
    const columns = Object.keys(totalOtherProblems[0][0]);

    for (let i = 0; i < totalOtherProblems.length; i += 1) {
      await postgreSQL`INSERT INTO public."otherProblem" ${postgreSQL(totalOtherProblems[i], columns)}`;
      console.log(`=== Inserted ${totalOtherProblems[i].length} otherProblems ===`);
    }
  });
}

export {importOtherProblems};