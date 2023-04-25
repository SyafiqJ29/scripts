import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// passport
const totalPassports = [];
let passports = [];
let rowCounter = 0;
let passportIdCounter = 1;
const importPassports = () => { 
  parseFile('./exports/tbl_worker_passport_details.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const passport = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_passport'][key]) {
        if (key === 'Passport_issue_date' || key === 'Passport_expiry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            passport[v1_v2_column_maps['tbl_passport'][key]] = null;
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            passport[v1_v2_column_maps['tbl_passport'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            passport[v1_v2_column_maps['tbl_passport'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // passport[v1_v2_column_maps['tbl_passport'][key]] = format(new Date(date), 'yyyy-MM-dd');

            passport[v1_v2_column_maps['tbl_passport'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          passport.worker_id = workerFINToId[row[key]];
        } else {
          passport[v1_v2_column_maps['tbl_passport'][key]] = row[key];
        }
      }
    });
    passport.date_last_updated = today;
    passport.created_by = 0;
    passport.id = passportIdCounter++;

    passports.push(passport);
    rowCounter++;

    if (rowCounter === 100) {
      totalPassports.push(passports);
      passports = [];
      rowCounter = 0;
    }
  })
  .on('end', async (rowCount) => {
    // insert all passports
    totalPassports.push(passports);
    const columns = Object.keys(totalPassports[0][0]);

    for (let i = 0; i < totalPassports.length; i += 1) {
      if (totalPassports[i].length > 0) await postgreSQL`INSERT INTO public.passport ${postgreSQL(totalPassports[i], columns)}`;
      console.log(`=== Inserted ${totalPassports[i].length} passports ===`);
    }
  });
}

export {importPassports};