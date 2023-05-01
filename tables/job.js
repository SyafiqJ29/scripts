import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { importProblems } from './problem.js'
import { workerFINToId } from './worker.js'
import { importCurrentPasses } from './currentPass.js';
import { importBenefits } from './benefit.js';
import { importIpaDetails } from './ipaDetails.js';
import { importVerbalAssurances } from './verbalAssurance.js';
import { importEmploymentContracts } from './employmentContract.js';
import { importAgents } from './agent.js';
import { importEmployers } from './employer.js';
import { importWorkplaces } from './workplace.js';
import { importWorkHistorys } from './workHistory.js';
import { importAccommodations } from './accommodation.js';
import { importTransferRepats } from './transferRepat.js';

// job key to id
export const jobKeyToId = {};

// worker id to jobs
const workerIdToJobs = {};

// job
const totalJobs = [];
let jobs = [];
let rowCounter = 0;
let jobIdCounter = 1;
const importJobs = () => { 
  parseFile('./exports/tbl_job.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const job = {};
    job.id = jobIdCounter++;
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_job'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            job[v1_v2_column_maps['tbl_job'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // job[v1_v2_column_maps['tbl_job'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            job[v1_v2_column_maps['tbl_job'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          job.worker_id = workerFINToId[row[key]];
        } else {
          job[v1_v2_column_maps['tbl_job'][key]] = row[key];
        }
      } else if (key ==='Job_key') {
        jobKeyToId[row[key]] = job.id;
      } 
    });

    if (!workerIdToJobs[job.worker_id]) workerIdToJobs[job.worker_id] = [];
    workerIdToJobs[job.worker_id].push(job.date_record_created);

    job.date_last_updated = today;
    job.created_by = 0;
    job.job_sequence = 0;
    jobs.push(job);
    rowCounter++;

    if (rowCounter === 100) {
      totalJobs.push(jobs);
      jobs = [];
      rowCounter = 0;
    }
  })
  .on('end', async (rowCount) => {
    // insert all jobs
    // totalJobs.push(jobs);
    
    // Object.keys(workerIdToJobs).forEach(key =>  {
    //   workerIdToJobs[key] = workerIdToJobs[key].sort();
    // });

    // for (let i = 0; i < totalJobs.length; i += 1) {
    //   for (let j = 0; j < totalJobs[i].length; j += 1) {
    //     for (let k = 0; k < workerIdToJobs[totalJobs[i][j].worker_id].length; k += 1) {
    //       if (totalJobs[i][j].date_record_created === workerIdToJobs[totalJobs[i][j].worker_id][k]) {
    //         totalJobs[i][j].job_sequence = k + 1;
    //         workerIdToJobs[totalJobs[i][j].worker_id][k] = '';
    //         break;
    //       }
    //     }
    //   }
    // }

    // const columns = Object.keys(totalJobs[0][0]);

    // for (let i = 0; i < totalJobs.length; i += 1) {
    //   if (totalJobs[i].length > 0) await postgreSQL`INSERT INTO public.job ${postgreSQL(totalJobs[i], columns)}`;
    //   console.log(`=== Inserted ${totalJobs[i].length} jobs ===`);
    // }

    // importCurrentPasses();
    // importBenefits();
    // importIpaDetails();
    // importVerbalAssurances();
    // importEmploymentContracts();
    // importAgents();
    // importEmployers();
    // importWorkplaces();
    // importWorkHistorys();
    // importAccommodations();
    // importTransferRepats();
    importProblems();
  });
}

export {importJobs};