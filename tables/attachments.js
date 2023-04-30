import * as fs from 'fs'
import { postgreSQL, today } from '../index.js'
import { parseFile } from 'fast-csv'
import { facepicAttachmentFileName, workerFINToId } from './worker.js'
import gm from 'gm'
const im = gm.subClass({ imageMagick: true })
import sharp from 'sharp'


const ordinaryAttachmentsPath = `/home/twc2/camans/server/files/ordinary-attachments`;
const facepicAttachmentsPath = `/home/twc2/camans/server/files/facepic-attachments`;

// attachments
const facepicTotalAttachments = [];
let facepicAttachments = [];
let facepicRowCounter = 0;
let facepicAttachmentIdCounter = 1;

let ordinaryTotalAttachments = [];
let ordinaryAttachments = [];
let ordinaryRowCounter = 0;
let ordinaryAttachmentIdCounter = 1;

let facepicPathToId = {};

const importAttachments = async () => {
  const directories = fs.readdirSync('workers');
  directories.forEach(directory => {
    const files = fs.readdirSync(`workers/${directory}`);
    files.forEach(file => {
      const stats = fs.statSync(`workers/${directory}/${file}`);

      let replacedFile = file.replace(/ /g, '-');
      replacedFile = replacedFile.replace(/#/g, '-');

      const attachment = {};
      attachment.date_record_created = today;
      attachment.date_last_updated = today;
      attachment.attachment_name = replacedFile;
      attachment.filename = replacedFile;
      attachment.worker_id = workerFINToId[directory];
      attachment.attachment_submitted_by = 0;
      attachment.file_size = stats.size;

      if (facepicAttachmentFileName[file]) {
        attachment.file_path = `${facepicAttachmentsPath}/${replacedFile}`;
        attachment.facepic_status = 'Current';
        attachment.id = facepicAttachmentIdCounter++;
        facepicPathToId[`${facepicAttachmentsPath}/${replacedFile}`] = attachment.id;

        if (attachment.worker_id) {
          facepicAttachments.push(attachment);
          facepicRowCounter++;

          if (facepicRowCounter === 100) {
            facepicTotalAttachments.push(facepicAttachments);
            facepicAttachments = [];
            facepicRowCounter = 0;
          }
        }
      } else {
        attachment.file_path = `${ordinaryAttachmentsPath}/${replacedFile}`;
        attachment.id = ordinaryAttachmentIdCounter++;

        if (attachment.worker_id) {
          ordinaryAttachments.push(attachment);
          ordinaryRowCounter++;

          if (ordinaryRowCounter === 100) {
            ordinaryTotalAttachments.push(ordinaryAttachments);
            ordinaryAttachments = [];
            ordinaryRowCounter = 0;
          }
        }
      }

      fs.copyFile(`workers/${directory}/${file}`, attachment.file_path, (err) => {
        if (err) throw err;
      });
    })
  });

  // insert all attachments
  facepicTotalAttachments.push(facepicAttachments);
  const facepicAttachmentColumns = Object.keys(facepicTotalAttachments[0][0]);

  for (let i = 0; i < facepicTotalAttachments.length; i += 1) {
    if (facepicTotalAttachments[i].length > 0) await postgreSQL`INSERT INTO public."facepicAttachment" ${postgreSQL(facepicTotalAttachments[i], facepicAttachmentColumns)}`;
    console.log(`=== Inserted ${facepicTotalAttachments[i].length} facepicAttachments ===`);

    for (let k = 0; k < facepicTotalAttachments[i].length; k += 1) {
      const worker = {
        path_current_facepic: facepicTotalAttachments[i][k].file_path
      }

      await postgreSQL`UPDATE public.worker SET ${postgreSQL(worker, 'path_current_facepic')} WHERE id=${facepicTotalAttachments[i][k].worker_id};`
    }
  }

  ordinaryTotalAttachments.push(ordinaryAttachments);
  const ordinaryAttachmentColumns = Object.keys(ordinaryTotalAttachments[0][0]);

  for (let i = 0; i < ordinaryTotalAttachments.length; i += 1) {
    if (ordinaryTotalAttachments[i].length > 0) await postgreSQL`INSERT INTO public."ordinaryAttachment" ${postgreSQL(ordinaryTotalAttachments[i], ordinaryAttachmentColumns)}`;
    console.log(`=== Inserted ${ordinaryTotalAttachments[i].length} ordinaryAttachments ===`);
  }

  resizeFacepicAttachments();
};

const resizeFacepicAttachments = async () => {
  const files = fs.readdirSync(facepicAttachmentsPath);

  for (let i = 0; i < files.length; i += 1) {
    const file = files[i];

    const filePath = `${facepicAttachmentsPath}/${file}`;

    console.log(filePath);
    if (filePath.indexOf('bmp') === -1) {
      await sharp(`${facepicAttachmentsPath}/${file}`)
        .resize({
          width: 600,
          height: 800
        })
        .toFile(`${facepicAttachmentsPath}/resized-${file}`);
  
      const stats = fs.statSync(`${facepicAttachmentsPath}/resized-${file}`);
  
      const facepicAttachment = {
        file_size: stats.size,
        file_path: `${facepicAttachmentsPath}/resized-${file}`
      }
  
      const path = `${facepicAttachmentsPath}/${file}`;
      if (facepicPathToId[path]) {
        await postgreSQL`UPDATE public."facepicAttachment" SET ${postgreSQL(facepicAttachment, ['file_size', 'file_path'])} WHERE id=${facepicPathToId[path]};`
        console.log(`=== Resized facepic ${i + 1} ===`);
      }
    } else {
      console.log(`=== Did not resize facepic ${i + 1} ===`);
    }
  }

  // files.forEach(file => {
  //   console.log("File: " + file);
  //   console.log("Filepath: " + `${facepicAttachmentsPath}/${file}`);
  //   im(`${facepicAttachmentsPath}/${file}`).resize(600, 800).write(`${facepicAttachmentsPath}/${file}`, async (err) => {
  //     console.log("Resized!")
  //     const stats = fs.statSync(`${facepicAttachmentsPath}/${file}`);

  //     const facepicAttachment = {
  //       file_size: stats.size
  //     }

  //     const path = `${facepicAttachmentsPath}/${file}`;
  //     if (facepicPathToId[path]) {
  //       await postgreSQL`UPDATE public."facepicAttachment" SET ${postgreSQL(facepicAttachment, 'file_size')} WHERE id=${facepicPathToId[path]};`
  //       console.log(`=== Resized facepic ===`);
  //     }
  //   });
  // })

  importOtherAttachments();
};

const importOtherAttachments = async () => {
  const ordinaryAttachmentSalaryClaimLodged = {};
  ordinaryTotalAttachments = [];
  ordinaryAttachments = [];
  ordinaryRowCounter = 0;

  parseFile('./exports/tbl_salary_claim_lodged.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const obj = {};
    let workerId = null;
    Object.keys(row).forEach(key => {
      if (key !== 'Prob_key' && key !== 'Job_key' && key !== 'ID') {
        if (key === 'Worker_FIN_number') {
          workerId = workerFINToId[row[key]];
        }
        obj[key] = row[key];
      }
    });

    if (workerId !== null) {
      if (!ordinaryAttachmentSalaryClaimLodged[workerId]) ordinaryAttachmentSalaryClaimLodged[workerId] = [];

      ordinaryAttachmentSalaryClaimLodged[workerId].push(obj);
    }
  })
  .on('end', async (rowCount) => {
    Object.keys(ordinaryAttachmentSalaryClaimLodged).forEach(workerId => {
      const fileName = `salary_claim_lodged_${ordinaryAttachmentSalaryClaimLodged[workerId][0]['Worker_FIN_number']}.csv`;
      let fileHeader = Object.keys(ordinaryAttachmentSalaryClaimLodged[workerId][0]);
      fileHeader = fileHeader.map(header => `"${header}"`);
      fileHeader = fileHeader.join(",");

      fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, fileHeader);
      fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, "\n");

      ordinaryAttachmentSalaryClaimLodged[workerId].forEach(ordinaryAttachmentSalaryClaimLodgedObj => {
        let content = Object.values(ordinaryAttachmentSalaryClaimLodgedObj);
        content = content.map(value => `"${value}"`);
        content = content.join(",");

        fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, content);
        fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, "\n");
      });

      const stats = fs.statSync(`${ordinaryAttachmentsPath}/${fileName}`);

      const attachment = {};
      attachment.date_record_created = today;
      attachment.date_last_updated = today;
      attachment.attachment_name = fileName;
      attachment.filename = fileName;
      attachment.worker_id = workerId;
      attachment.attachment_submitted_by = 0;
      attachment.file_size = stats.size;
      attachment.file_path = `${ordinaryAttachmentsPath}/${fileName}`;
      attachment.id = ordinaryAttachmentIdCounter++;

      ordinaryAttachments.push(attachment);
      ordinaryRowCounter++;

      if (ordinaryRowCounter === 100) {
        ordinaryTotalAttachments.push(ordinaryAttachments);
        ordinaryAttachments = [];
        ordinaryRowCounter = 0;
      }
    });

    // insert ordinaryAttachments
    ordinaryTotalAttachments.push(ordinaryAttachments);
    const ordinaryAttachmentColumns = Object.keys(ordinaryTotalAttachments[0][0]);

    for (let i = 0; i < ordinaryTotalAttachments.length; i += 1) {
      if (ordinaryTotalAttachments[i].length > 0) await postgreSQL`INSERT INTO public."ordinaryAttachment" ${postgreSQL(ordinaryTotalAttachments[i], ordinaryAttachmentColumns)}`;
      console.log(`=== Inserted ${ordinaryTotalAttachments[i].length} salaryClaimLodged ===`);
    }

    importNonWicaClaim();
  });
};

const importNonWicaClaim = async () => {
  const ordinaryAttachmentNonWicaClaim = {};
  ordinaryTotalAttachments = [];
  ordinaryAttachments = [];
  ordinaryRowCounter = 0;

  parseFile('./exports/tbl_non_wica_claim.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const obj = {};
    let workerId = null;
    Object.keys(row).forEach(key => {
      if (key !== 'Prob_key' && key !== 'Job_key' && key !== 'ID') {
        if (key === 'Worker_FIN_number') {
          workerId = workerFINToId[row[key]];
        }
        obj[key] = row[key];
      }
    });

    if (workerId !== null) {
      if (!ordinaryAttachmentNonWicaClaim[workerId]) ordinaryAttachmentNonWicaClaim[workerId] = [];

      ordinaryAttachmentNonWicaClaim[workerId].push(obj);
    }
  })
  .on('end', async (rowCount) => {
    Object.keys(ordinaryAttachmentNonWicaClaim).forEach(workerId => {
      const fileName = `non_wica_claim_${ordinaryAttachmentNonWicaClaim[workerId][0]['Worker_FIN_number']}.csv`;
      let fileHeader = Object.keys(ordinaryAttachmentNonWicaClaim[workerId][0]);
      fileHeader = fileHeader.map(header => `"${header}"`);
      fileHeader = fileHeader.join(",");

      fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, fileHeader);
      fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, "\n");

      ordinaryAttachmentNonWicaClaim[workerId].forEach(ordinaryAttachmentNonWicaClaim => {
        let content = Object.values(ordinaryAttachmentNonWicaClaim);
        content = content.map(value => `"${value}"`);
        content = content.join(",");

        fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, content);
        fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, "\n");
      });

      const stats = fs.statSync(`${ordinaryAttachmentsPath}/${fileName}`);

      const attachment = {};
      attachment.date_record_created = today;
      attachment.date_last_updated = today;
      attachment.attachment_name = fileName;
      attachment.filename = fileName;
      attachment.worker_id = workerId;
      attachment.attachment_submitted_by = 0;
      attachment.file_size = stats.size;
      attachment.file_path = `${ordinaryAttachmentsPath}/${fileName}`;
      attachment.id = ordinaryAttachmentIdCounter++;

      ordinaryAttachments.push(attachment);
      ordinaryRowCounter++;

      if (ordinaryRowCounter === 100) {
        ordinaryTotalAttachments.push(ordinaryAttachments);
        ordinaryAttachments = [];
        ordinaryRowCounter = 0;
      }
    });

    // insert ordinaryAttachments
    ordinaryTotalAttachments.push(ordinaryAttachments);
    const ordinaryAttachmentColumns = Object.keys(ordinaryTotalAttachments[0][0]);

    for (let i = 0; i < ordinaryTotalAttachments.length; i += 1) {
      if (ordinaryTotalAttachments[i].length > 0) await postgreSQL`INSERT INTO public."ordinaryAttachment" ${postgreSQL(ordinaryTotalAttachments[i], ordinaryAttachmentColumns)}`;
      console.log(`=== Inserted ${ordinaryTotalAttachments[i].length} nonWicaClaim ===`);
    }

    importR2R();
  });
};

const importR2R = async () => {
  const ordinaryAttachmentR2R = {};
  ordinaryTotalAttachments = [];
  ordinaryAttachments = [];
  ordinaryRowCounter = 0;

  parseFile('./exports/tbl_R2R.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const obj = {};
    let workerId = null;
    Object.keys(row).forEach(key => {
      if (key !== 'Prob_key' && key !== 'Job_key' && key !== 'ID') {
        if (key === 'Worker_FIN_number') {
          workerId = workerFINToId[row[key]];
        }
        obj[key] = row[key];
      }
    });

    if (workerId !== null) {
      if (!ordinaryAttachmentR2R[workerId]) ordinaryAttachmentR2R[workerId] = [];

      ordinaryAttachmentR2R[workerId].push(obj);
    }
  })
  .on('end', async (rowCount) => {
    Object.keys(ordinaryAttachmentR2R).forEach(workerId => {
      const fileName = `r2r_${ordinaryAttachmentR2R[workerId][0]['Worker_FIN_number']}.csv`;
      let fileHeader = Object.keys(ordinaryAttachmentR2R[workerId][0]);
      fileHeader = fileHeader.map(header => `"${header}"`);
      fileHeader = fileHeader.join(",");

      fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, fileHeader);
      fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, "\n");

      ordinaryAttachmentR2R[workerId].forEach(ordinaryAttachmentR2R => {
        let content = Object.values(ordinaryAttachmentR2R);
        content = content.map(value => `"${value}"`);
        content = content.join(",");

        fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, content);
        fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, "\n");
      });

      const stats = fs.statSync(`${ordinaryAttachmentsPath}/${fileName}`);

      const attachment = {};
      attachment.date_record_created = today;
      attachment.date_last_updated = today;
      attachment.attachment_name = fileName;
      attachment.filename = fileName;
      attachment.worker_id = workerId;
      attachment.attachment_submitted_by = 0;
      attachment.file_size = stats.size;
      attachment.file_path = `${ordinaryAttachmentsPath}/${fileName}`;
      attachment.id = ordinaryAttachmentIdCounter++;

      ordinaryAttachments.push(attachment);
      ordinaryRowCounter++;

      if (ordinaryRowCounter === 100) {
        ordinaryTotalAttachments.push(ordinaryAttachments);
        ordinaryAttachments = [];
        ordinaryRowCounter = 0;
      }
    });

    // insert ordinaryAttachments
    ordinaryTotalAttachments.push(ordinaryAttachments);
    const ordinaryAttachmentColumns = Object.keys(ordinaryTotalAttachments[0][0]);

    for (let i = 0; i < ordinaryTotalAttachments.length; i += 1) {
      if (ordinaryTotalAttachments[i].length > 0) await postgreSQL`INSERT INTO public."ordinaryAttachment" ${postgreSQL(ordinaryTotalAttachments[i], ordinaryAttachmentColumns)}`;
      console.log(`=== Inserted ${ordinaryTotalAttachments[i].length} r2r ===`);
    }

    importCaseMilestoneNonCriminal();
  });
};

const importCaseMilestoneNonCriminal = async () => {
  const ordinaryAttachmentCaseMilestoneNonCriminal = {};
  ordinaryTotalAttachments = [];
  ordinaryAttachments = [];
  ordinaryRowCounter = 0;

  parseFile('./exports/tbl_casemilestone_noncriminal.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const obj = {};
    let workerId = null;
    Object.keys(row).forEach(key => {
      if (key !== 'Prob_key' && key !== 'Job_key' && key !== 'ID') {
        if (key === 'Worker_FIN_number') {
          workerId = workerFINToId[row[key]];
        }
        obj[key] = row[key];
      }
    });

    if (workerId !== null) {
      if (!ordinaryAttachmentCaseMilestoneNonCriminal[workerId]) ordinaryAttachmentCaseMilestoneNonCriminal[workerId] = [];

      ordinaryAttachmentCaseMilestoneNonCriminal[workerId].push(obj);
    }
  })
  .on('end', async (rowCount) => {
    Object.keys(ordinaryAttachmentCaseMilestoneNonCriminal).forEach(workerId => {
      const fileName = `r2r_${ordinaryAttachmentCaseMilestoneNonCriminal[workerId][0]['Worker_FIN_number']}.csv`;
      let fileHeader = Object.keys(ordinaryAttachmentCaseMilestoneNonCriminal[workerId][0]);
      fileHeader = fileHeader.map(header => `"${header}"`);
      fileHeader = fileHeader.join(",");

      fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, fileHeader);
      fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, "\n");

      ordinaryAttachmentCaseMilestoneNonCriminal[workerId].forEach(ordinaryAttachmentCaseMilestoneNonCriminal => {
        let content = Object.values(ordinaryAttachmentCaseMilestoneNonCriminal);
        content = content.map(value => `"${value}"`);
        content = content.join(",");

        fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, content);
        fs.appendFileSync(`${ordinaryAttachmentsPath}/${fileName}`, "\n");
      });

      const stats = fs.statSync(`${ordinaryAttachmentsPath}/${fileName}`);

      const attachment = {};
      attachment.date_record_created = today;
      attachment.date_last_updated = today;
      attachment.attachment_name = fileName;
      attachment.filename = fileName;
      attachment.worker_id = workerId;
      attachment.attachment_submitted_by = 0;
      attachment.file_size = stats.size;
      attachment.file_path = `${ordinaryAttachmentsPath}/${fileName}`;
      attachment.id = ordinaryAttachmentIdCounter++;

      ordinaryAttachments.push(attachment);
      ordinaryRowCounter++;

      if (ordinaryRowCounter === 100) {
        ordinaryTotalAttachments.push(ordinaryAttachments);
        ordinaryAttachments = [];
        ordinaryRowCounter = 0;
      }
    });

    // insert ordinaryAttachments
    ordinaryTotalAttachments.push(ordinaryAttachments);
    const ordinaryAttachmentColumns = Object.keys(ordinaryTotalAttachments[0][0]);

    for (let i = 0; i < ordinaryTotalAttachments.length; i += 1) {
      if (ordinaryTotalAttachments[i].length > 0) await postgreSQL`INSERT INTO public."ordinaryAttachment" ${postgreSQL(ordinaryTotalAttachments[i], ordinaryAttachmentColumns)}`;
      console.log(`=== Inserted ${ordinaryTotalAttachments[i].length} caseMilestoneNonCriminal ===`);
    }
  });
};

export {importAttachments};
