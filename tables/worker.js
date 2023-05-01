import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { importJobs } from './job.js'
import { importNicknames } from './nickname.js'
import { importPassports } from './passport.js'
import { importSgPhoneNumbers } from './sgPhoneNumber.js'
import { importDigitalContacts } from './digitalContact.js'
import { importHomeCountryPhoneNumbers } from './homeCountryPhoneNumber.js'
import { importSgAddresss } from './sgAddress.js'
import { importhomeCountryAddresses } from './homeCountryAddress.js'
import { importNextOfKins } from './nextOfKin.js'
import { importFamilyMembers } from './familyMember.js'
import { importSgFriends } from './sgFriend.js'
import { importLanguages } from './language.js'
import { importBankAccounts } from './bankAccount.js'
import { importAttachments } from './attachments.js'

// worker fin to id
export const workerFINToId = {};

// facepicAttachment to true
export const facepicAttachmentFileName = {};

// worker
const totalWorkers = [];
let workers = [];
const twidCounter = {};
let rowCounter = 0;
let workerIdCounter = 1;
const importWorkers = () => { 
  parseFile('./exports/tbl_worker.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const worker = {};
    let photoFileName;
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_worker'][key]) {
        if (key === 'Worker_registration_date' || key === 'Date_of_birth') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            worker[v1_v2_column_maps['tbl_worker'][key]] = '1920-01-01';
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            worker[v1_v2_column_maps['tbl_worker'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            worker[v1_v2_column_maps['tbl_worker'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // worker[v1_v2_column_maps['tbl_worker'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            worker[v1_v2_column_maps['tbl_worker'][key]] = row[key];
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            worker[v1_v2_column_maps['tbl_worker'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // worker[v1_v2_column_maps['tbl_worker'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            worker[v1_v2_column_maps['tbl_worker'][key]] = row[key];
          }
        } else {
          worker[v1_v2_column_maps['tbl_worker'][key]] = row[key];
        }
      } else if (key === 'Photo') {
        const splittedPhotoFileName = row[key].split('/');
        photoFileName = splittedPhotoFileName[splittedPhotoFileName.length - 1];
        facepicAttachmentFileName[photoFileName] = true;
      }
    });
    worker.date_last_updated = today;
    worker.created_by = 0;
    worker.id = workerIdCounter++;
    workerFINToId[worker.fin_number] = worker.id;
    facepicAttachmentFileName[photoFileName] = worker.id;

    let twidCounterKey = `${worker.fin_number.slice(-4)}${getYear(new Date(worker.date_of_birth)).toString().slice(-2)}`;
    (twidCounter[twidCounterKey] === undefined) ? twidCounter[twidCounterKey] = 1 : twidCounter[twidCounterKey]++;
    if (twidCounter[twidCounterKey] < 10) twidCounter[twidCounterKey] = `0${twidCounter[twidCounterKey]}`;
    
    worker.twid = `${twidCounterKey}${twidCounter[twidCounterKey]}`;
    workers.push(worker);
    rowCounter++;

    if (rowCounter === 100) {
      totalWorkers.push(workers);
      workers = [];
      rowCounter = 0;
    }
  })
  .on('end', async (rowCount) => {
    // insert all workers
    // totalWorkers.push(workers);
    // const columns = Object.keys(totalWorkers[0][0]);

    // for (let i = 0; i < totalWorkers.length; i += 1) {
    //   if (totalWorkers[i].length > 0) await postgreSQL`INSERT INTO public.worker ${postgreSQL(totalWorkers[i], columns)}`;
    //   console.log(`=== Inserted ${totalWorkers[i].length} workers ===`);
    // }

    // importAttachments();

    // importNicknames();
    // importPassports();
    importSgPhoneNumbers();
    // importDigitalContacts();
    // importHomeCountryPhoneNumbers();
    // importSgAddresss();
    // importhomeCountryAddresses();
    // importNextOfKins();
    // importFamilyMembers();
    // importSgFriends();
    // importLanguages();
    // importBankAccounts();

    // importJobs();
  });
}

export {importWorkers};