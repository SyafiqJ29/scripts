import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// transferRepat
const totalTransferRepats = [];
let transferRepats = [];
let rowCounter = 0;
let transferRepatIdCounter = 1;
const importTransferRepats = () => { 
  parseFile('./exports/tbl_ttr.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const transferRepat = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_transferRepat'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            transferRepat[v1_v2_column_maps['tbl_transferRepat'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // transferRepat[v1_v2_column_maps['tbl_transferRepat'][key]] = format(new Date(date), 'yyyy-MM-dd');

            transferRepat[v1_v2_column_maps['tbl_transferRepat'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          transferRepat.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          transferRepat.worker_id = workerFINToId[row[key]];
        } else {
          transferRepat[v1_v2_column_maps['tbl_transferRepat'][key]] = row[key];
        }
      }
    });
    transferRepat.date_last_updated = today;
    transferRepat.created_by = 0;
    transferRepat.id = transferRepatIdCounter++;

    if (transferRepat.worker_id && transferRepat.job_id) {
      transferRepats.push(transferRepat);
      rowCounter++;

      if (rowCounter === 100) {
        totalTransferRepats.push(transferRepats);
        transferRepats = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all transferRepats
    totalTransferRepats.push(transferRepats);
    const columns = Object.keys(totalTransferRepats[0][0]);

    for (let i = 0; i < totalTransferRepats.length; i += 1) {
      if (totalTransferRepats[i].length > 0) await postgreSQL`INSERT INTO public."transferRepat" ${postgreSQL(totalTransferRepats[i], columns)}`;
      console.log(`=== Inserted ${totalTransferRepats[i].length} transferRepats ===`);
    }
  });
}

export {importTransferRepats};