import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// familyMember
const totalFamilyMembers = [];
let familyMembers = [];
let rowCounter = 0;
let familyMemberIdCounter = 1;
const importFamilyMembers = () => { 
  parseFile('./exports/tbl_family_member.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const familyMember = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_familyMember'][key]) {
        if (key === 'Family_member_obsolete') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            familyMember[v1_v2_column_maps['tbl_familyMember'][key]] = null;
          } else {
            let dateParts = row[key].split("-");
            let date = format(new Date(+dateParts[2], dateParts[1], +dateParts[0]), 'yyyy-MM-dd');
            familyMember[v1_v2_column_maps['tbl_familyMember'][key]] = date;
          }
        } else if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            familyMember[v1_v2_column_maps['tbl_familyMember'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // familyMember[v1_v2_column_maps['tbl_familyMember'][key]] = date;

            familyMember[v1_v2_column_maps['tbl_familyMember'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          familyMember.worker_id = workerFINToId[row[key]];
        } else {
          familyMember[v1_v2_column_maps['tbl_familyMember'][key]] = row[key];
        }
      }
    });
    familyMember.date_last_updated = today;
    familyMember.created_by = 0;
    familyMember.id = familyMemberIdCounter++;

    if (familyMember.worker_id) {
      familyMembers.push(familyMember);
      rowCounter++;

      if (rowCounter === 100) {
        totalFamilyMembers.push(familyMembers);
        familyMembers = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all familyMembers
    totalFamilyMembers.push(familyMembers);
    const columns = Object.keys(totalFamilyMembers[0][0]);

    for (let i = 0; i < totalFamilyMembers.length; i += 1) {
      await postgreSQL`INSERT INTO public."familyMember" ${postgreSQL(totalFamilyMembers[i], columns)}`;
      console.log(`=== Inserted ${totalFamilyMembers[i].length} familyMembers ===`);
    }
  });
}

export {importFamilyMembers};