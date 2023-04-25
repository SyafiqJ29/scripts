import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// otherComplaint
const totalOtherComplaints = [];
let OtherComplaints = [];
let rowCounter = 0;
let otherComplaintIdCounter = 1;
const importOtherComplaints = () => { 
  parseFile('./exports/tbl_other_complaint.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const otherComplaint = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_otherComplaint'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            otherComplaint[v1_v2_column_maps['tbl_otherComplaint'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // otherComplaint[v1_v2_column_maps['tbl_otherComplaint'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            otherComplaint[v1_v2_column_maps['tbl_otherComplaint'][key]] = row[key];
          }
        } else if (key === 'Other_plaint_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            otherComplaint[v1_v2_column_maps['tbl_otherComplaint'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            otherComplaint[v1_v2_column_maps['tbl_otherComplaint'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');
          }
        } else if (key === 'Prob_key') {
          otherComplaint.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          otherComplaint.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          otherComplaint.worker_id = workerFINToId[row[key]];
        } else {
          otherComplaint[v1_v2_column_maps['tbl_otherComplaint'][key]] = row[key];
        }
      }
    });
    otherComplaint.date_last_updated = today;
    otherComplaint.created_by = 0;
    otherComplaint.id = otherComplaintIdCounter++;

    if (otherComplaint.worker_id && otherComplaint.job_id && otherComplaint.problem_id) {
      OtherComplaints.push(otherComplaint);
      rowCounter++;

      if (rowCounter === 100) {
        totalOtherComplaints.push(OtherComplaints);
        OtherComplaints = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all OtherComplaints
    totalOtherComplaints.push(OtherComplaints);
    const columns = Object.keys(totalOtherComplaints[0][0]);

    for (let i = 0; i < totalOtherComplaints.length; i += 1) {
      if (totalOtherComplaints[i].length > 0) await postgreSQL`INSERT INTO public."otherComplaint" ${postgreSQL(totalOtherComplaints[i], columns)}`;
      console.log(`=== Inserted ${totalOtherComplaints[i].length} OtherComplaints ===`);
    }
  });
}

export {importOtherComplaints};