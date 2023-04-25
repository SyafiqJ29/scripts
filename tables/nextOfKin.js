import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// nextOfKin
const totalNextOfKins = [];
let nextOfKins = [];
let rowCounter = 0;
let nextOfKinIdCounter = 1;
const importNextOfKins = () => { 
  parseFile('./exports/tbl_kin.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const nextOfKin = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_nextOfKin'][key]) {
        if (key === 'Kin_obsolete_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            nextOfKin[v1_v2_column_maps['tbl_nextOfKin'][key]] = null;
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            nextOfKin[v1_v2_column_maps['tbl_nextOfKin'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            nextOfKin[v1_v2_column_maps['tbl_nextOfKin'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // nextOfKin[v1_v2_column_maps['tbl_nextOfKin'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            nextOfKin[v1_v2_column_maps['tbl_nextOfKin'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          nextOfKin.worker_id = workerFINToId[row[key]];
        } else {
          nextOfKin[v1_v2_column_maps['tbl_nextOfKin'][key]] = row[key];
        }
      }
    });
    nextOfKin.date_last_updated = today;
    nextOfKin.created_by = 0;
    nextOfKin.id = nextOfKinIdCounter++;

    if (nextOfKin.worker_id) {
      nextOfKins.push(nextOfKin);
      rowCounter++;

      if (rowCounter === 100) {
        totalNextOfKins.push(nextOfKins);
        nextOfKins = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all nextOfKins
    totalNextOfKins.push(nextOfKins);
    const columns = Object.keys(totalNextOfKins[0][0]);

    for (let i = 0; i < totalNextOfKins.length; i += 1) {
      if (totalNextOfKins[i].length > 0) await postgreSQL`INSERT INTO public."nextOfKin" ${postgreSQL(totalNextOfKins[i], columns)}`;
      console.log(`=== Inserted ${totalNextOfKins[i].length} nextOfKins ===`);
    }
  });
}

export {importNextOfKins};