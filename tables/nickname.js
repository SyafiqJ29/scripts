import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// nickname
const totalNicknames = [];
let nicknames = [];
let rowCounter = 0;
let nicknameIdCounter = 1;
const importNicknames = () => { 
  parseFile('./exports/tbl_worker_nickname.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const nickname = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_nickname'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            nickname[v1_v2_column_maps['tbl_nickname'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // nickname[v1_v2_column_maps['tbl_nickname'][key]] = date;

            nickname[v1_v2_column_maps['tbl_nickname'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          nickname.worker_id = workerFINToId[row[key]];
        } else {
          nickname[v1_v2_column_maps['tbl_nickname'][key]] = row[key];
        }
      }
    });
    nickname.date_last_updated = today;
    nickname.created_by = 0;
    nickname.id = nicknameIdCounter++;

    if (nickname.worker_id) {
      nicknames.push(nickname);
      rowCounter++;

      if (rowCounter === 100) {
        totalNicknames.push(nicknames);
        nicknames = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all nicknames
    totalNicknames.push(nicknames);
    const columns = Object.keys(totalNicknames[0][0]);

    for (let i = 0; i < totalNicknames.length; i += 1) {
      await postgreSQL`INSERT INTO public.nickname ${postgreSQL(totalNicknames[i], columns)}`;
      console.log(`=== Inserted ${totalNicknames[i].length} nicknames ===`);
    }
  });
}

export {importNicknames};