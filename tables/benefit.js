import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// benefit
const totalBenefits = [];
let benefits = [];
let rowCounter = 0;
let benefitIdCounter = 1;
const importBenefits = () => { 
  parseFile('./exports/tbl_benefit.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const benefit = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_benefit'][key]) {
        if (key === 'Bene_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            benefit[v1_v2_column_maps['tbl_benefit'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            benefit[v1_v2_column_maps['tbl_benefit'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            benefit[v1_v2_column_maps['tbl_benefit'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // benefit[v1_v2_column_maps['tbl_benefit'][key]] = format(new Date(date), 'yyyy-MM-dd');

            benefit[v1_v2_column_maps['tbl_benefit'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          benefit.job_id = jobKeyToId[row[key]];
        }  else if (key === 'Worker_FIN_number') {
          benefit.worker_id = workerFINToId[row[key]];
        } else {
          benefit[v1_v2_column_maps['tbl_benefit'][key]] = row[key];
        }
      }
    });

    if (benefit.bene_item === 'Shelter admission' || benefit.bene_item === 'Cash for accommodation' || benefit.bene_item === 'Project roof allowance' || benefit.bene_item === 'Hotel invoice') {
      benefit.bene_class = 'Accommodation';
    } else if (benefit.bene_item === 'Tablet' || benefit.bene_item === 'Phone value card' || benefit.bene_item === 'Mobile device' || benefit.bene_item === 'Phone top-up') {
      benefit.bene_class = 'Connectivity';
    } else if (benefit.bene_item === 'Dorm meal allowance' || benefit.bene_item === 'Meal card' || benefit.bene_item === 'Other food beneft' || benefit.bene_item === 'Cash for meal') {
      benefit.bene_class = 'Food';
    } else if (benefit.bene_item === 'Cash for dental' || benefit.bene_item === 'Other medical benefit' || benefit.bene_item === 'Cash for medical' || benefit.bene_item === 'Hospital invoice') {
      benefit.bene_class = 'Medical';
    } else if (benefit.bene_item === 'Other miscellaneous' || benefit.bene_item === 'Cash for other purposes' || benefit.bene_item === 'Legal/court costs' || benefit.bene_item === 'Invoice from other vendor' || benefit.bene_item === 'Airline ticket' || benefit.bene_item === 'Remittance to worker or family' || benefit.bene_item === 'Refund by worker') {
      benefit.bene_class = 'Other';
    } else if (benefit.bene_item === 'Farego cash' || benefit.bene_item === 'Farego transport allowance' || benefit.bene_item === 'Farego card' || benefit.bene_item === 'Taxi fare') {
      benefit.bene_class = 'Transport';
    } else {
      benefit.bene_class = 'Other';
    }

    benefit.date_last_updated = today;
    benefit.created_by = 0;
    benefit.id = benefitIdCounter++;

    if (benefit.worker_id && benefit.job_id) {
      benefits.push(benefit);
      rowCounter++;

      if (rowCounter === 100) {
        totalBenefits.push(benefits);
        benefits = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all benefits
    totalBenefits.push(benefits);
    const columns = Object.keys(totalBenefits[0][0]);

    for (let i = 0; i < totalBenefits.length; i += 1) {
      if (totalBenefits[i].length > 0) await postgreSQL`INSERT INTO public."benefit" ${postgreSQL(totalBenefits[i], columns)}`;
      console.log(`=== Inserted ${totalBenefits[i].length} benefits ===`);
    }
  });
}

export {importBenefits};