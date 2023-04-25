import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// aggravatingIssue
const totalAggravatingIssues = [];
let aggravatingIssues = [];
let rowCounter = 0;
let aggravatingIssueIdCounter = 1;
const importAggravatingIssues = () => { 
  parseFile('./exports/tbl_aggravating_issue.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const aggravatingIssue = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_aggravatingIssue'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            aggravatingIssue[v1_v2_column_maps['tbl_aggravatingIssue'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // aggravatingIssue[v1_v2_column_maps['tbl_aggravatingIssue'][key]] = format(new Date(date), 'yyyy-MM-dd');

            aggravatingIssue[v1_v2_column_maps['tbl_aggravatingIssue'][key]] = row[key];
          }
        } else if (key === 'Aggra_loss') {
          if (row[key] !== "") aggravatingIssue.aggra_loss = parseFloat(row[key]);
        } else if (key === 'Prob_key') {
          aggravatingIssue.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          aggravatingIssue.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          aggravatingIssue.worker_id = workerFINToId[row[key]];
        } else {
          aggravatingIssue[v1_v2_column_maps['tbl_aggravatingIssue'][key]] = row[key];
        }
      }
    });
    aggravatingIssue.date_last_updated = today;
    aggravatingIssue.created_by = 0;
    aggravatingIssue.id = aggravatingIssueIdCounter++;

    if (aggravatingIssue.worker_id && aggravatingIssue.job_id && aggravatingIssue.problem_id) {
      aggravatingIssues.push(aggravatingIssue);
      rowCounter++;

      if (rowCounter === 100) {
        totalAggravatingIssues.push(aggravatingIssues);
        aggravatingIssues = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all aggravatingIssues
    totalAggravatingIssues.push(aggravatingIssues);
    const columns = Object.keys(totalAggravatingIssues[0][0]);

    for (let i = 0; i < totalAggravatingIssues.length; i += 1) {
      if (totalAggravatingIssues[i].length > 0) await postgreSQL`INSERT INTO public."aggravatingIssue" ${postgreSQL(totalAggravatingIssues[i], columns)}`;
      console.log(`=== Inserted ${totalAggravatingIssues[i].length} aggravatingIssues ===`);
    }
  });
}

export {importAggravatingIssues};