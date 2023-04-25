import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';
import { problemKeyToId } from './problem.js';

// wicaClaim
const totalWicaClaims = [];
let wicaClaims = [];
let rowCounter = 0;
let wicaClaimIdCounter = 1;

// insurer
const totalInsurers = [];
let insurers = [];
let insurerRowCounter = 0;
let insurerIdCounter = 1;

const importWicaClaims = () => { 
  parseFile('./exports/tbl_wica_claim.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const wicaClaim = {};
    const insurer = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_wicaClaim'][key]) {
        if (key === 'Wica_claim_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            wicaClaim[v1_v2_column_maps['tbl_wicaClaim'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            wicaClaim[v1_v2_column_maps['tbl_wicaClaim'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            wicaClaim[v1_v2_column_maps['tbl_wicaClaim'][key]] = '1920-01-01 00:00:00';
            insurer[v1_v2_column_maps['tbl_wicaClaim'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // wicaClaim[v1_v2_column_maps['tbl_wicaClaim'][key]] = format(new Date(date), 'yyyy-MM-dd');

            wicaClaim[v1_v2_column_maps['tbl_wicaClaim'][key]] = row[key];
            insurer[v1_v2_column_maps['tbl_wicaClaim'][key]] = row[key];
          }
        } else if (key === 'Prob_key') {
          wicaClaim.problem_id = problemKeyToId[row[key]];
          insurer.problem_id = problemKeyToId[row[key]];
        } else if (key === 'Job_key') {
          wicaClaim.job_id = jobKeyToId[row[key]];
          insurer.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          wicaClaim.worker_id = workerFINToId[row[key]];
          insurer.worker_id = workerFINToId[row[key]];
        } else {
          wicaClaim[v1_v2_column_maps['tbl_wicaClaim'][key]] = row[key];
        }
      } else if (key === 'Wica_insurer') {
        if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
          insurer.insurance_company = 'no info at migration';
        } else {
          insurer.insurance_company = row[key];
        }
      } else if (key === 'Wica_policy_nbr') {
        if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
          insurer.insurance_policy_number = 'no info at migration';
        } else {
          insurer.insurance_policy_number = row[key];
        }
      }
    });
    wicaClaim.date_last_updated = today;
    wicaClaim.created_by = 0;
    wicaClaim.id = wicaClaimIdCounter++;

    insurer.insurance_type = 'no info at migration';
    insurer.date_last_updated = today;
    insurer.created_by = 0;
    insurer.id = insurerIdCounter++;

    if (wicaClaim.worker_id && wicaClaim.job_id && wicaClaim.problem_id) {
      wicaClaims.push(wicaClaim);
      rowCounter++;

      if (rowCounter === 100) {
        totalWicaClaims.push(wicaClaims);
        wicaClaims = [];
        rowCounter = 0;
      }
    }

    if (insurer.worker_id && insurer.job_id && insurer.problem_id) {
      insurers.push(insurer);
      insurerRowCounter++;

      if (insurerRowCounter === 100) {
        totalInsurers.push(insurers);
        insurers = [];
        insurerRowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all wicaClaims
    totalWicaClaims.push(wicaClaims);
    const columns = Object.keys(totalWicaClaims[0][0]);

    for (let i = 0; i < totalWicaClaims.length; i += 1) {
      if (totalWicaClaims[i].length > 0) await postgreSQL`INSERT INTO public."wicaClaim" ${postgreSQL(totalWicaClaims[i], columns)}`;
      console.log(`=== Inserted ${totalWicaClaims[i].length} wicaClaims ===`);
    }

    // insert all insurers
    totalInsurers.push(insurers);
    const insurerColumns = Object.keys(totalInsurers[0][0]);

    for (let i = 0; i < totalInsurers.length; i += 1) {
      if (totalInsurers[i].length > 0) await postgreSQL`INSERT INTO public."insurer" ${postgreSQL(totalInsurers[i], insurerColumns)}`;
      console.log(`=== Inserted ${totalInsurers[i].length} insurers ===`);
    }
  });
}

export {importWicaClaims};