import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// homeCountryPhoneNumber
const totalHomeCountryPhoneNumbers = [];
let homeCountryPhoneNumbers = [];
let rowCounter = 0;
let homeCountryPhoneNumberIdCounter = 1;
const importHomeCountryPhoneNumbers = () => { 
  parseFile('./exports/tbl_home_country_phone_number.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const homeCountryPhoneNumber = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_homeCountryPhoneNumber'][key]) {
        if (key === 'Home_phone_obsolete') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            homeCountryPhoneNumber[v1_v2_column_maps['tbl_homeCountryPhoneNumber'][key]] = null;
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            homeCountryPhoneNumber[v1_v2_column_maps['tbl_homeCountryPhoneNumber'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            homeCountryPhoneNumber[v1_v2_column_maps['tbl_homeCountryPhoneNumber'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // homeCountryPhoneNumber[v1_v2_column_maps['tbl_homeCountryPhoneNumber'][key]] = date;

            homeCountryPhoneNumber[v1_v2_column_maps['tbl_homeCountryPhoneNumber'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          homeCountryPhoneNumber.worker_id = workerFINToId[row[key]];
        } else {
          homeCountryPhoneNumber[v1_v2_column_maps['tbl_homeCountryPhoneNumber'][key]] = row[key];
        }
      }
    });
    homeCountryPhoneNumber.date_last_updated = today;
    homeCountryPhoneNumber.created_by = 0;
    homeCountryPhoneNumber.id = homeCountryPhoneNumberIdCounter++;

    if (homeCountryPhoneNumber.worker_id) {
      homeCountryPhoneNumbers.push(homeCountryPhoneNumber);
      rowCounter++;

      if (rowCounter === 100) {
        totalHomeCountryPhoneNumbers.push(homeCountryPhoneNumbers);
        homeCountryPhoneNumbers = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all homeCountryPhoneNumbers
    totalHomeCountryPhoneNumbers.push(homeCountryPhoneNumbers);
    const columns = Object.keys(totalHomeCountryPhoneNumbers[0][0]);

    for (let i = 0; i < totalHomeCountryPhoneNumbers.length; i += 1) {
      if (totalHomeCountryPhoneNumbers[i].length > 0) await postgreSQL`INSERT INTO public."homeCountryPhoneNumber" ${postgreSQL(totalHomeCountryPhoneNumbers[i], columns)}`;
      console.log(`=== Inserted ${totalHomeCountryPhoneNumbers[i].length} homeCountryPhoneNumbers ===`);
    }
  });
}

export {importHomeCountryPhoneNumbers};