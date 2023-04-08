import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// accommodation
const totalAccommodations = [];
let accommodations = [];
let rowCounter = 0;
let accommodationIdCounter = 1;
const importAccommodations = () => { 
  parseFile('./exports/tbl_accomodation.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const accommodation = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_accommodation'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            accommodation[v1_v2_column_maps['tbl_accommodation'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // accommodation[v1_v2_column_maps['tbl_accommodation'][key]] = date;

            accommodation[v1_v2_column_maps['tbl_accommodation'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          accommodation.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          accommodation.worker_id = workerFINToId[row[key]];
        } else {
          accommodation[v1_v2_column_maps['tbl_accommodation'][key]] = row[key];
        }
      }
    });
    accommodation.date_last_updated = today;
    accommodation.created_by = 0;
    accommodation.id = accommodationIdCounter++;

    if (accommodation.worker_id && accommodation.job_id) {
      accommodations.push(accommodation);
      rowCounter++;

      if (rowCounter === 100) {
        totalAccommodations.push(accommodations);
        accommodations = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all accommodations
    totalAccommodations.push(accommodations);
    const columns = Object.keys(totalAccommodations[0][0]);

    for (let i = 0; i < totalAccommodations.length; i += 1) {
      await postgreSQL`INSERT INTO public."accommodation" ${postgreSQL(totalAccommodations[i], columns)}`;
      console.log(`=== Inserted ${totalAccommodations[i].length} accommodations ===`);
    }
  });
}

export {importAccommodations};