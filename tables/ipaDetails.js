import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// ipaDetail
const totalIpaDetails = [];
let ipaDetails = [];
let rowCounter = 0;
let ipaDetailIdCounter = 1;
const importIpaDetails = () => { 
  parseFile('./exports/tbl_ipa_details.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const ipaDetail = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_ipaDetails'][key]) {
        if (key === 'IPA_application_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            ipaDetail[v1_v2_column_maps['tbl_ipaDetails'][key]] = null;
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            ipaDetail[v1_v2_column_maps['tbl_ipaDetails'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            ipaDetail[v1_v2_column_maps['tbl_ipaDetails'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // ipaDetail[v1_v2_column_maps['tbl_ipaDetails'][key]] = date;

            ipaDetail[v1_v2_column_maps['tbl_ipaDetails'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          ipaDetail.job_id = jobKeyToId[row[key]];
        }  else if (key === 'Worker_FIN_number') {
          ipaDetail.worker_id = workerFINToId[row[key]];
        } else {
          ipaDetail[v1_v2_column_maps['tbl_ipaDetails'][key]] = row[key];
        }
      }
    });
    ipaDetail.date_last_updated = today;
    ipaDetail.created_by = 0;
    ipaDetail.id = ipaDetailIdCounter++;

    if (ipaDetail.worker_id && ipaDetail.job_id) {
      ipaDetails.push(ipaDetail);
      rowCounter++;

      if (rowCounter === 100) {
        totalIpaDetails.push(ipaDetails);
        ipaDetails = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all ipaDetails
    totalIpaDetails.push(ipaDetails);
    const columns = Object.keys(totalIpaDetails[0][0]);

    for (let i = 0; i < totalIpaDetails.length; i += 1) {
      await postgreSQL`INSERT INTO public."ipaDetails" ${postgreSQL(totalIpaDetails[i], columns)}`;
      console.log(`=== Inserted ${totalIpaDetails[i].length} ipaDetails ===`);
    }
  });
}

export {importIpaDetails};