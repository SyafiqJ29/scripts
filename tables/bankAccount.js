import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// bankAccount
const totalBankAccounts = [];
let bankAccounts = [];
let rowCounter = 0;
let bankAccountIdCounter = 1;
const importBankAccounts = () => { 
  parseFile('./exports/tbl_bank_acc_details.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const bankAccount = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_bankAccount'][key]) {
        if (key === 'Bank_obsolete_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            bankAccount[v1_v2_column_maps['tbl_bankAccount'][key]] = null;
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            bankAccount[v1_v2_column_maps['tbl_bankAccount'][key]] = format(new Date(date), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            bankAccount[v1_v2_column_maps['tbl_bankAccount'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // bankAccount[v1_v2_column_maps['tbl_bankAccount'][key]] = format(new Date(date), 'yyyy-MM-dd');

            bankAccount[v1_v2_column_maps['tbl_bankAccount'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          bankAccount.worker_id = workerFINToId[row[key]];
        } else {
          bankAccount[v1_v2_column_maps['tbl_bankAccount'][key]] = row[key];
        }
      }
    });
    bankAccount.date_last_updated = today;
    bankAccount.created_by = 0;
    bankAccount.id = bankAccountIdCounter++;

    if (bankAccount.worker_id) {
      bankAccounts.push(bankAccount);
      rowCounter++;

      if (rowCounter === 100) {
        totalBankAccounts.push(bankAccounts);
        bankAccounts = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all bankAccounts
    totalBankAccounts.push(bankAccounts);
    const columns = Object.keys(totalBankAccounts[0][0]);

    for (let i = 0; i < totalBankAccounts.length; i += 1) {
      if (totalBankAccounts[i].length > 0) await postgreSQL`INSERT INTO public."bankAccount" ${postgreSQL(totalBankAccounts[i], columns)}`;
      console.log(`=== Inserted ${totalBankAccounts[i].length} bankAccounts ===`);
    }
  });
}

export {importBankAccounts};