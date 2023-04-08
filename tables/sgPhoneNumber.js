import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// sgPhoneNumber
const totalSgPhoneNumbers = [];
let sgPhoneNumbers = [];
let rowCounter = 0;
let sgPhoneNumberIdCounter = 1;
const importSgPhoneNumbers = () => { 
  parseFile('./exports/tbl_sg_phone_number.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const sgPhoneNumber = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_sgPhoneNumber'][key]) {
        if (key === 'Sg_phone_obsolete') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            sgPhoneNumber[v1_v2_column_maps['tbl_sgPhoneNumber'][key]] = null;
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            sgPhoneNumber[v1_v2_column_maps['tbl_sgPhoneNumber'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            sgPhoneNumber[v1_v2_column_maps['tbl_sgPhoneNumber'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // sgPhoneNumber[v1_v2_column_maps['tbl_sgPhoneNumber'][key]] = date;

            sgPhoneNumber[v1_v2_column_maps['tbl_sgPhoneNumber'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          sgPhoneNumber.worker_id = workerFINToId[row[key]];
        } else {
          sgPhoneNumber[v1_v2_column_maps['tbl_sgPhoneNumber'][key]] = row[key];
        }
      }
    });
    sgPhoneNumber.date_last_updated = today;
    sgPhoneNumber.created_by = 0;
    sgPhoneNumber.id = sgPhoneNumberIdCounter++;

    if (sgPhoneNumber.worker_id) {
      sgPhoneNumbers.push(sgPhoneNumber);
      rowCounter++;

      if (rowCounter === 100) {
        totalSgPhoneNumbers.push(sgPhoneNumbers);
        sgPhoneNumbers = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all sgPhoneNumbers
    totalSgPhoneNumbers.push(sgPhoneNumbers);
    const columns = Object.keys(totalSgPhoneNumbers[0][0]);

    for (let i = 0; i < totalSgPhoneNumbers.length; i += 1) {
      await postgreSQL`INSERT INTO public."sgPhoneNumber" ${postgreSQL(totalSgPhoneNumbers[i], columns)}`;
      console.log(`=== Inserted ${totalSgPhoneNumbers[i].length} sgPhoneNumbers ===`);
    }
  });
}

export {importSgPhoneNumbers};