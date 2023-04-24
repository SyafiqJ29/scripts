import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'

// language
const totalLanguages = [];
let languages = [];
let rowCounter = 0;
let languageIdCounter = 1;
const importLanguages = () => { 
  parseFile('./exports/tbl_language.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const language = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_language'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            language[v1_v2_column_maps['tbl_language'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // language[v1_v2_column_maps['tbl_language'][key]] = date;

            language[v1_v2_column_maps['tbl_language'][key]] = row[key];
          }
        } else if (key === 'Worker_FIN_number') {
          language.worker_id = workerFINToId[row[key]];
        } else {
          language[v1_v2_column_maps['tbl_language'][key]] = row[key];
        }
      }
    });
    language.date_last_updated = today;
    language.created_by = 0;
    language.id = languageIdCounter++;

    if (language.worker_id) {
      languages.push(language);
      rowCounter++;

      if (rowCounter === 100) {
        totalLanguages.push(languages);
        languages = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all languages
    totalLanguages.push(languages);
    const columns = Object.keys(totalLanguages[0][0]);

    for (let i = 0; i < totalLanguages.length; i += 1) {
      if (totalLanguages[i].length > 0) await postgreSQL`INSERT INTO public."language" ${postgreSQL(totalLanguages[i], columns)}`;
      console.log(`=== Inserted ${totalLanguages[i].length} languages ===`);
    }
  });
}

export {importLanguages};