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
import { importAttachments, importOtherAttachments } from './attachments.js'

const existingTwids = ['366K8801','388X8901','382L9001','398K9201','373L9101','518M9301','595P9401','579M9501','615T8301','565L8801','607V9001','739W9301','713X9201','769W8601','225K9501','222M9601','449U8901','349T9001','231T9201','473R8701','427Q8701','523V9401','398T9501','707N8901','896P8701','840Q9001','956R9101','806W9401','869P9301','933Q9201','867R9501','234L9101','344U8901','751W9001','675V9101','831X9201','103N9201','565M8501','479P8801','655Q8901','430R9001','600R9201','111T8601','211U9401','390V8301','815K8601','135X9501','612X9101','373P8601','631K9401','933N9301','655R9302','602R9301','550V8901','499L9201','144T8901','679M9101','123N9001','465P9201','444R9201','950L8501','298R8801','161W8201','560X9101','535T9001','131W9101','777P9201','123N9003','354T8601','688W9001','791U9101','432T9301','655R9301','114T8501','388X8903','388X8904','409Q8001','604Q8801','137T9301','440W9501','429R9001','318K8901','505U9101','692V9401','226P9401','283P9801','015T9001','716X8801','539V9201','335M9501','694V9401','228K9201','608W9101','321Q9201','GEN847608','423U9601','GEN521132','GEN163694','372B0201','650A0401','780T8501','567Z2501','991P9401','344U8902','032U8701','008W9001','392F8601','881L8901','456W0001','690P4301','374P9101','763W8201','069N8901','686T8101','GEN101303','GEN323762','919C9301','431Z8301','GEN270099','GEN867123','515F9801','227U9901','119T9001','849X9001','272P9501','946T9801','606N8801','366K8803','170Z5401','650A8701','650A8702','GEN625812','GEN565682','GEN226603','GEN177929','590A3701','GEN478244','GEN552186','GEN698123','059T8401','778T9201','GEN837188','GEN048043','GEN434275','123H7001','650A9701','GEN071613','GEN781236','515F9802','392F8001','GEN112614','366K8802','722T9201','181M9201','GEN926646','A123456A','999Z9876','999Z9877','999Z9878','999Z9879','GEN952384','GEN109812','594X9201','323L9401','345T9001','345T9201','678U9001','678U9301','226W8701','227W9501','244V9101','032U8786','015P9301','244V9102','388X89021','174L9001','GEN125466','123Z9601','650A9801','032U8780','032U8781','032U8782','032U8783','032U8784','032U8785','032U8787','032U8788','032U8790','106T9001','401X9101','401X9201','799K8801','799K9201','950L8502','002V9201','003Z9301','001W9101','004W9401','950L9301','121M9301','121M9302','139N9601','139U9701','GEN906295','508W8901','115K9101','296P9701','296P9801','057Q9801','088R9701','088R9801','555R8901','219P9101','663K9001','057Q9301','236L9201','353M9301','474N9401','591P9501','622Q9601','747R9701','868T9801','989U9901','001V9001','117W9101','246X9301','375K9501','404L9701','521M9901','652N9001','783Q9201','833P9401','968T9601','098R9801','199V9101','289U9401','371X9701','463W9001','555L9301','647K9601','739N9801','822M9801','914P9201','096Q9301','109K8501','220L9001','236L9202','615Q9201','978U9501','462N8901','584P8701','757R9301','117W9102','833P9402','404L9702','463W9002','521M9902','858T9401','444T9501','737A9901','900B9901','341M8601','215L9601','196M8901','401N9301','583U9401','205W9601','068P8101','917T9601','448V2301','GEN552096','339T9401','679K9001','406U9901','456U0001','388X8902','999A5601']

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
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            worker[v1_v2_column_maps['tbl_worker'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            worker[v1_v2_column_maps['tbl_worker'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // worker[v1_v2_column_maps['tbl_worker'][key]] = date;

            worker[v1_v2_column_maps['tbl_worker'][key]] = row[key];
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            worker[v1_v2_column_maps['tbl_worker'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // worker[v1_v2_column_maps['tbl_worker'][key]] = date;

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
    if (existingTwids.indexOf(worker.twid) === -1) {
      workers.push(worker);
      rowCounter++;

      if (rowCounter === 100) {
        totalWorkers.push(workers);
        workers = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all workers
    totalWorkers.push(workers);
    const columns = Object.keys(totalWorkers[0][0]);

    for (let i = 0; i < totalWorkers.length; i += 1) {
      await postgreSQL`INSERT INTO public.worker ${postgreSQL(totalWorkers[i], columns)}`;
      console.log(`=== Inserted ${totalWorkers[i].length} workers ===`);
    }

    importAttachments();

    importNicknames();
    importPassports();
    importSgPhoneNumbers();
    importDigitalContacts();
    importHomeCountryPhoneNumbers();
    importSgAddresss();
    importhomeCountryAddresses();
    importNextOfKins();
    importFamilyMembers();
    importSgFriends();
    importLanguages();
    importBankAccounts();

    importJobs();
  });
}

export {importWorkers};