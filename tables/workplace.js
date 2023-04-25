import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// workplace
const totalWorkplaces = [];
let workplaces = [];
let rowCounter = 0;
let workplaceIdCounter = 1;
const importWorkplaces = () => { 
  parseFile('./exports/tbl_workplace.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const workplace = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_workplace'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            workplace[v1_v2_column_maps['tbl_workplace'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // workplace[v1_v2_column_maps['tbl_workplace'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            workplace[v1_v2_column_maps['tbl_workplace'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          workplace.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          workplace.worker_id = workerFINToId[row[key]];
        } else {
          workplace[v1_v2_column_maps['tbl_workplace'][key]] = row[key];
        }
      }
    });
    workplace.date_last_updated = today;
    workplace.created_by = 0;
    workplace.id = workplaceIdCounter++;

    if (workplace.worker_id && workplace.job_id) {
      workplaces.push(workplace);
      rowCounter++;

      if (rowCounter === 100) {
        totalWorkplaces.push(workplaces);
        workplaces = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all workplaces
    totalWorkplaces.push(workplaces);
    const columns = Object.keys(totalWorkplaces[0][0]);

    for (let i = 0; i < totalWorkplaces.length; i += 1) {
      if (totalWorkplaces[i].length > 0) await postgreSQL`INSERT INTO public."workplace" ${postgreSQL(totalWorkplaces[i], columns)}`;
      console.log(`=== Inserted ${totalWorkplaces[i].length} workplaces ===`);
    }
  });
}

export {importWorkplaces};