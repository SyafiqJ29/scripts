import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// homeCountryAddress
const totalhomeCountryAddresses = [];
let homeCountryAddresses = [];
let rowCounter = 0;
let homeCountryAddressIdCounter = 1;
const importhomeCountryAddresses = () => { 
  parseFile('./exports/tbl_home_country_address.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const homeCountryAddress = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_homeCountryAddress'][key]) {
        if (key === 'Home_country_obsolete') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            homeCountryAddress[v1_v2_column_maps['tbl_homeCountryAddress'][key]] = null;
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            homeCountryAddress[v1_v2_column_maps['tbl_homeCountryAddress'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            homeCountryAddress[v1_v2_column_maps['tbl_homeCountryAddress'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // homeCountryAddress[v1_v2_column_maps['tbl_homeCountryAddress'][key]] = format(new Date(date), 'yyyy-MM-dd');

            homeCountryAddress[v1_v2_column_maps['tbl_homeCountryAddress'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          homeCountryAddress.worker_id = workerFINToId[row[key]];
        } else {
          homeCountryAddress[v1_v2_column_maps['tbl_homeCountryAddress'][key]] = row[key];
        }
      }
    });
    homeCountryAddress.date_last_updated = today;
    homeCountryAddress.created_by = 0;
    homeCountryAddress.id = homeCountryAddressIdCounter++;

    if (homeCountryAddress.worker_id) {
      homeCountryAddresses.push(homeCountryAddress);
      rowCounter++;

      if (rowCounter === 100) {
        totalhomeCountryAddresses.push(homeCountryAddresses);
        digitalContacts = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all homeCountryAddresses
    totalhomeCountryAddresses.push(homeCountryAddresses);
    const columns = Object.keys(totalhomeCountryAddresses[0][0]);

    for (let i = 0; i < totalhomeCountryAddresses.length; i += 1) {
      if (totalhomeCountryAddresses[i].length > 0) await postgreSQL`INSERT INTO public."homeCountryAddress" ${postgreSQL(totalhomeCountryAddresses[i], columns)}`;
      console.log(`=== Inserted ${totalhomeCountryAddresses[i].length} homeCountryAddresses ===`);
    }
  });
}

export {importhomeCountryAddresses};