import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// caseDiscussion
const totalCaseDiscussions = [];
let caseDiscussions = [];
let rowCounter = 0;
let caseDiscussionIdCounter = 1;
const importCaseDiscussions = () => { 
  parseFile('./exports/tbl_case_discussion.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const caseDiscussion = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_caseDiscussion'][key]) {
        if (key === 'Discuss_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            caseDiscussion[v1_v2_column_maps['tbl_caseDiscussion'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            caseDiscussion[v1_v2_column_maps['tbl_caseDiscussion'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            caseDiscussion[v1_v2_column_maps['tbl_caseDiscussion'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // caseDiscussion[v1_v2_column_maps['tbl_caseDiscussion'][key]] = format(new Date(date), 'yyyy-MM-dd');

            caseDiscussion[v1_v2_column_maps['tbl_caseDiscussion'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          caseDiscussion.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          caseDiscussion.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          caseDiscussion.worker_id = workerFINToId[row[key]];
        } else {
          caseDiscussion[v1_v2_column_maps['tbl_caseDiscussion'][key]] = row[key];
        }
      }
    });
    caseDiscussion.date_last_updated = today;
    caseDiscussion.created_by = 0;
    caseDiscussion.id = caseDiscussionIdCounter++;

    if (caseDiscussion.worker_id && caseDiscussion.job_id && caseDiscussion.problem_id) {
      caseDiscussions.push(caseDiscussion);
      rowCounter++;

      if (rowCounter === 100) {
        totalCaseDiscussions.push(caseDiscussions);
        caseDiscussions = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all caseDiscussions
    totalCaseDiscussions.push(caseDiscussions);
    const columns = Object.keys(totalCaseDiscussions[0][0]);

    for (let i = 0; i < totalCaseDiscussions.length; i += 1) {
      if (totalCaseDiscussions[i].length > 0) await postgreSQL`INSERT INTO public."caseDiscussion" ${postgreSQL(totalCaseDiscussions[i], columns)}`;
      console.log(`=== Inserted ${totalCaseDiscussions[i].length} caseDiscussions ===`);
    }
  });
}

export {importCaseDiscussions};