import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// criminalCaseMilestone
const totalCriminalCaseMilestones = [];
let criminalCaseMilestones = [];
let rowCounter = 0;
let criminalCaseMilestoneIdCounter = 1;
const importCriminalCaseMilestones = () => { 
  parseFile('./exports/tbl_casemilestone_criminal.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const criminalCaseMilestone = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_criminalCaseMilestone'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            criminalCaseMilestone[v1_v2_column_maps['tbl_criminalCaseMilestone'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // criminalCaseMilestone[v1_v2_column_maps['tbl_criminalCaseMilestone'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            criminalCaseMilestone[v1_v2_column_maps['tbl_criminalCaseMilestone'][key]] = row[key];
          }
        } else if (key === 'Miles_cr_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            criminalCaseMilestone[v1_v2_column_maps['tbl_criminalCaseMilestone'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            criminalCaseMilestone[v1_v2_column_maps['tbl_criminalCaseMilestone'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');
          }
        } else if (key === 'Prob_key') {
          criminalCaseMilestone.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          criminalCaseMilestone.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          criminalCaseMilestone.worker_id = workerFINToId[row[key]];
        } else {
          criminalCaseMilestone[v1_v2_column_maps['tbl_criminalCaseMilestone'][key]] = row[key];
        }
      }
    });
    criminalCaseMilestone.date_last_updated = today;
    criminalCaseMilestone.created_by = 0;
    criminalCaseMilestone.id = criminalCaseMilestoneIdCounter++;

    if (criminalCaseMilestone.worker_id && criminalCaseMilestone.job_id && criminalCaseMilestone.problem_id) {
      criminalCaseMilestones.push(criminalCaseMilestone);
      rowCounter++;

      if (rowCounter === 100) {
        totalCriminalCaseMilestones.push(criminalCaseMilestones);
        criminalCaseMilestones = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all criminalCaseMilestones
    totalCriminalCaseMilestones.push(criminalCaseMilestones);
    const columns = Object.keys(totalCriminalCaseMilestones[0][0]);

    for (let i = 0; i < totalCriminalCaseMilestones.length; i += 1) {
      if (totalCriminalCaseMilestones[i].length > 0) await postgreSQL`INSERT INTO public."criminalCaseMilestone" ${postgreSQL(totalCriminalCaseMilestones[i], columns)}`;
      console.log(`=== Inserted ${totalCriminalCaseMilestones[i].length} criminalCaseMilestones ===`);
    }
  });
}

export {importCriminalCaseMilestones};