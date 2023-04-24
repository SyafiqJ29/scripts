import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// policeReport
const totalPoliceReports = [];
let policeReports = [];
let rowCounter = 0;
let policeReportIdCounter = 1;
const importPoliceReports = () => { 
  parseFile('./exports/tbl_police_report.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const policeReport = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_policeReport'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            policeReport[v1_v2_column_maps['tbl_policeReport'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // policeReport[v1_v2_column_maps['tbl_policeReport'][key]] = date;

            policeReport[v1_v2_column_maps['tbl_policeReport'][key]] = row[key];
          }
        } else if (key === 'Police_rpt_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            policeReport[v1_v2_column_maps['tbl_policeReport'][key]] = '1920-01-01';
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            policeReport[v1_v2_column_maps['tbl_policeReport'][key]] = date;
          }
        } else if (key === 'Prob_key') {
          policeReport.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          policeReport.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          policeReport.worker_id = workerFINToId[row[key]];
        } else {
          policeReport[v1_v2_column_maps['tbl_policeReport'][key]] = row[key];
        }
      }
    });
    policeReport.date_last_updated = today;
    policeReport.created_by = 0;
    policeReport.id = policeReportIdCounter++;

    if (policeReport.worker_id && policeReport.job_id && policeReport.problem_id) {
      policeReports.push(policeReport);
      rowCounter++;

      if (rowCounter === 100) {
        totalPoliceReports.push(policeReports);
        policeReports = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all policeReports
    totalPoliceReports.push(policeReports);
    const columns = Object.keys(totalPoliceReports[0][0]);

    for (let i = 0; i < totalPoliceReports.length; i += 1) {
      if (totalPoliceReports[i].length > 0) await postgreSQL`INSERT INTO public."policeReport" ${postgreSQL(totalPoliceReports[i], columns)}`;
      console.log(`=== Inserted ${totalPoliceReports[i].length} policeReports ===`);
    }
  });
}

export {importPoliceReports};