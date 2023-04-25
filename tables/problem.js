import { parseFile } from 'fast-csv'
import { postgreSQL, v1_v2_column_maps, today } from '../index.js'
import { jobKeyToId } from './job.js'
import { workerFINToId } from './worker.js'
import { isBefore } from 'date-fns'
import { importAggravatingIssues } from './aggravatingIssue.js'
import { importLeadCaseWorkers } from './leadCaseWorker.js'
import { importAuxiliaryCaseWorkers } from './auxiliaryCaseWorker.js'
import { importCaseDiscussions } from './caseDiscussion.js'
import { importLawyers } from './lawyer.js'
import { importSalaryHistorys } from './salaryHistory.js'
import { importInjurys } from './injury.js'
import { importIllnesss } from './illness.js'
import { importHospitals } from './hospital.js'
import { importWicaClaims } from './wicaClaim.js'
import { importWicaStatuses } from './wicaStatus.js'
import { importMcStatuses } from './mcStatus.js'
import { importOtherProblems } from './otherProblem.js'
import { importPoliceReports } from './policeReport.js'
import { importOtherComplaints } from './otherComplaint.js'
import { importCriminalCaseMilestones } from './criminalCaseMilestone.js'

// problem key to id
export const problemKeyToId = {};

// worker id to problems
const workerIdToProblems = {};

// problem
const totalProblems = [];
let problems = [];
let rowCounter = 0;
let problemIdCounter = 1;
const importProblems = () => { 
  parseFile('./exports/tbl_problem.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const problem = {};
    problem.id = problemIdCounter++;
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_problem'][key]) {
        if (key === 'Entry_date') {
          if (row[key] === 'NULL' || row[key] === '' || row[key] === ' ') {
            problem[v1_v2_column_maps['tbl_problem'][key]] = '1920-01-01 00:00:00';
          } else {
            // let dateTimeParts = row[key].split(" ");
            // let dateParts = dateTimeParts[0].split("-");
            // let date = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]} ${dateTimeParts[1]}`;
            // problem[v1_v2_column_maps['tbl_problem'][key]] = format(new Date(row[key]), 'yyyy-MM-dd');

            problem[v1_v2_column_maps['tbl_problem'][key]] = row[key];
          }
        } else {
          problem[v1_v2_column_maps['tbl_problem'][key]] = row[key];
        }
      } else if (key ==='Job_key') {
        problem.job_id = jobKeyToId[row[key]];
      } else if (key === 'Worker_FIN_number') {
        // console.log(row[key]);
        // console.log(workerFINToId[row[key]]);
        // console.log("=======")
        problem.worker_id = workerFINToId[row[key]];
      } else if (key ==='Prob_key') {
        problemKeyToId[row[key]] = problem.id;
      }
    });

    if (isBefore(new Date(problem.date_problem_registered), new Date(2021-12-31))) {
      problem.problem_status = 'Closed';
    } else {
      problem.problem_status = 'Dormant';
    }

    if (!workerIdToProblems[problem.worker_id]) workerIdToProblems[problem.worker_id] = [];
    workerIdToProblems[problem.worker_id].push(problem.date_record_created);

    problem.date_last_updated = today;
    problem.created_by = 0;
    problem.problem_sequence = 0;
    problems.push(problem);
    rowCounter++;

    if (rowCounter === 100) {
      totalProblems.push(problems);
      problems = [];
      rowCounter = 0;
    }
  })
  .on('end', async (rowCount) => {
    // insert all problems
    totalProblems.push(problems);

    Object.keys(workerIdToProblems).forEach(key =>  {
      workerIdToProblems[key] = workerIdToProblems[key].sort();
    });

    for (let i = 0; i < totalProblems.length; i += 1) {
      for (let j = 0; j < totalProblems[i].length; j += 1) {
        for (let k = 0; k < workerIdToProblems[totalProblems[i][j].worker_id].length; k += 1) {
          if (totalProblems[i][j].date_record_created === workerIdToProblems[totalProblems[i][j].worker_id][k]) {
            totalProblems[i][j].problem_sequence = k + 1;
            workerIdToProblems[totalProblems[i][j].worker_id][k] = '';
            break;
          }
        }
      }
    }

    const columns = Object.keys(totalProblems[0][0]);

    for (let i = 0; i < totalProblems.length; i += 1) {
      if (totalProblems[i].length > 0) await postgreSQL`INSERT INTO public.problem ${postgreSQL(totalProblems[i], columns)}`;
      console.log(`=== Inserted ${totalProblems[i].length} problems ===`);
    }

    importAggravatingIssues();
    importLeadCaseWorkers();
    importAuxiliaryCaseWorkers();
    importCaseDiscussions();
    importLawyers();
    importSalaryHistorys();
    importInjurys();
    importIllnesss();
    importHospitals();
    importWicaClaims();
    importWicaStatuses();
    importMcStatuses();
    importOtherProblems();
    importPoliceReports();
    importOtherComplaints();
    importCriminalCaseMilestones();
  });
}

export {importProblems};