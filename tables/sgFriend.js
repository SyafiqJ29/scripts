import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// sgFriend
const totalSgFriends = [];
let sgFriends = [];
let rowCounter = 0;
let sgFriendIdCounter = 1;
const importSgFriends = () => { 
  parseFile('./exports/tbl_sg_friend.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const sgFriend = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_sgFriend'][key]) {
        if (key === 'friend_sg_obsolete') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            sgFriend[v1_v2_column_maps['tbl_sgFriend'][key]] = null;
          } else {
            // let dateParts = row[key].split("-");
            // let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            sgFriend[v1_v2_column_maps['tbl_sgFriend'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            sgFriend[v1_v2_column_maps['tbl_sgFriend'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // sgFriend[v1_v2_column_maps['tbl_sgFriend'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            sgFriend[v1_v2_column_maps['tbl_sgFriend'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          sgFriend.worker_id = workerFINToId[row[key]];
        } else {
          sgFriend[v1_v2_column_maps['tbl_sgFriend'][key]] = row[key];
        }
      }
    });
    sgFriend.date_last_updated = today;
    sgFriend.created_by = 0;
    sgFriend.id = sgFriendIdCounter++;

    if (sgFriend.worker_id) {
      sgFriends.push(sgFriend);
      rowCounter++;

      if (rowCounter === 100) {
        totalSgFriends.push(sgFriends);
        sgFriends = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all sgFriends
    totalSgFriends.push(sgFriends);
    const columns = Object.keys(totalSgFriends[0][0]);

    for (let i = 0; i < totalSgFriends.length; i += 1) {
      if (totalSgFriends[i].length > 0) await postgreSQL`INSERT INTO public."sgFriend" ${postgreSQL(totalSgFriends[i], columns)}`;
      console.log(`=== Inserted ${totalSgFriends[i].length} sgFriends ===`);
    }
  });
}

export {importSgFriends};