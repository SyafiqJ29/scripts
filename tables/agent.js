import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { getYear, format } from 'date-fns'
import { workerFINToId } from './worker.js'
import { jobKeyToId } from './job.js';

// agent
const totalAgents = [];
let agents = [];
let rowCounter = 0;
let agentIdCounter = 1;
const importAgents = () => { 
  parseFile('./exports/tbl_agent.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const agent = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_agent'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            agent[v1_v2_column_maps['tbl_agent'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // agent[v1_v2_column_maps['tbl_agent'][key]] = date;

            agent[v1_v2_column_maps['tbl_agent'][key]] = row[key];
          }
        } else if (key === 'Job_key') {
          agent.job_id = jobKeyToId[row[key]];
        } else if (key === 'Worker_FIN_number') {
          agent.worker_id = workerFINToId[row[key]];
        } else {
          agent[v1_v2_column_maps['tbl_agent'][key]] = row[key];
        }
      }
    });
    agent.date_last_updated = today;
    agent.created_by = 0;
    agent.id = agentIdCounter++;

    if (agent.worker_id && agent.job_id) {
      agents.push(agent);
      rowCounter++;

      if (rowCounter === 100) {
        totalAgents.push(agents);
        agents = [];
        rowCounter = 0;
      }
    }
  })
  .on('end', async (rowCount) => {
    // insert all agents
    totalAgents.push(agents);
    const columns = Object.keys(totalAgents[0][0]);

    for (let i = 0; i < totalAgents.length; i += 1) {
      await postgreSQL`INSERT INTO public."agent" ${postgreSQL(totalAgents[i], columns)}`;
      console.log(`=== Inserted ${totalAgents[i].length} agents ===`);
    }
  });
}

export {importAgents};