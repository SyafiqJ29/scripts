import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// digitalContact
const totalDigitalContacts = [];
let digitalContacts = [];
let rowCounter = 0;
let digitalContactIdCounter = 1;
const importDigitalContacts = () => { 
  parseFile('./exports/tbl_digital_contact.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const digitalContact = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_digitalContact'][key]) {
        if (key === 'Obsolete_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            digitalContact[v1_v2_column_maps['tbl_digitalContact'][key]] = null;
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            digitalContact[v1_v2_column_maps['tbl_digitalContact'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            digitalContact[v1_v2_column_maps['tbl_digitalContact'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // digitalContact[v1_v2_column_maps['tbl_digitalContact'][key]] = format(new Date(date), 'yyyy-MM-dd');

            digitalContact[v1_v2_column_maps['tbl_digitalContact'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          digitalContact.worker_id = workerFINToId[row[key]];
        } else {
          digitalContact[v1_v2_column_maps['tbl_digitalContact'][key]] = row[key];
        }
      }
    });
    digitalContact.date_last_updated = today;
    digitalContact.created_by = 0;
    digitalContact.id = digitalContactIdCounter++;

    if (digitalContact.worker_id) {
      digitalContacts.push(digitalContact);
      rowCounter++;

      if (rowCounter === 100) {
        totalDigitalContacts.push(digitalContacts);
        digitalContacts = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all digitalContacts
    totalDigitalContacts.push(digitalContacts);
    const columns = Object.keys(totalDigitalContacts[0][0]);

    for (let i = 0; i < totalDigitalContacts.length; i += 1) {
      if (totalDigitalContacts[i].length > 0) await postgreSQL`INSERT INTO public."digitalContact" ${postgreSQL(totalDigitalContacts[i], columns)}`;
      console.log(`=== Inserted ${totalDigitalContacts[i].length} digitalContacts ===`);
    }
  });
}

export {importDigitalContacts};