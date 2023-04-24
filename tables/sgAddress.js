import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// sgAddress
const totalSgAddresss = [];
let sgAddresss = [];
let rowCounter = 0;
let sgAddressIdCounter = 1;
const importSgAddresss = () => { 
  parseFile('./exports/tbl_sg_address.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const sgAddress = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_sgAddress'][key]) {
        if (key === 'Singapore_address_obsolete') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            sgAddress[v1_v2_column_maps['tbl_sgAddress'][key]] = null;
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            sgAddress[v1_v2_column_maps['tbl_sgAddress'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            sgAddress[v1_v2_column_maps['tbl_sgAddress'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // sgAddress[v1_v2_column_maps['tbl_sgAddress'][key]] = date;

            sgAddress[v1_v2_column_maps['tbl_sgAddress'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          sgAddress.worker_id = workerFINToId[row[key]];
        } else {
          sgAddress[v1_v2_column_maps['tbl_sgAddress'][key]] = row[key];
        }
      }
    });
    sgAddress.date_last_updated = today;
    sgAddress.created_by = 0;
    sgAddress.id = sgAddressIdCounter++;

    if (sgAddress.worker_id) {
      sgAddresss.push(sgAddress);
      rowCounter++;

      if (rowCounter === 100) {
        totalSgAddresss.push(sgAddresss);
        sgAddresss = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all sgAddresss
    totalSgAddresss.push(sgAddresss);
    const columns = Object.keys(totalSgAddresss[0][0]);

    for (let i = 0; i < totalSgAddresss.length; i += 1) {
      if (totalSgAddresss[i].length > 0) await postgreSQL`INSERT INTO public."sgAddress" ${postgreSQL(totalSgAddresss[i], columns)}`;
      console.log(`=== Inserted ${totalSgAddresss[i].length} sgAddresss ===`);
    }
  });
}

export {importSgAddresss};