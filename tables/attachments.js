import * as fs from 'fs'
import { postgreSQL, today } from '../index.js'
import { facepicAttachmentFileName, workerFINToId } from './worker.js'

const ordinaryAttachmentsPath = `/home/twc2/camans/server/files/ordinary-attachments`;
const facepicAttachmentsPath = `/home/twc2/camans/server/files/facepic-attachments`;

const importAttachments = async () => {
  // attachments
  const facepicTotalAttachments = [];
  let facepicAttachments = [];
  let facepicRowCounter = 0;
  let facepicAttachmentIdCounter = 1;

  const ordinaryTotalAttachments = [];
  let ordinaryAttachments = [];
  let ordinaryRowCounter = 0;
  let ordinaryAttachmentIdCounter = 1;

  let copyIndex = 1;

  const directories = fs.readdirSync('workers');
  directories.forEach(directory => {
    const files = fs.readdirSync(`workers/${directory}`);
    files.forEach( file => {
      const stats = fs.statSync(`workers/${directory}/${file}`);

      const attachment = {};
      attachment.date_record_created = today;
      attachment.date_last_updated = today;
      attachment.attachment_name = file;
      attachment.filename = file;
      attachment.worker_id = workerFINToId[directory];
      attachment.attachment_submitted_by = 0;
      attachment.file_size = stats.size;

      if (facepicAttachmentFileName[file]) {
        attachment.file_path = `${facepicAttachmentsPath}/${file}`;
        attachment.facepic_status = 'Current';
        attachment.id = facepicAttachmentIdCounter++;

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
        attachment.file_path = `${ordinaryAttachmentsPath}/${file}`;
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
        console.log(`copied file: ${copyIndex++}`);
      });
    })
  });

  // insert all attachments
  facepicTotalAttachments.push(facepicAttachments);
  const facepicAttachmentColumns = Object.keys(facepicTotalAttachments[0][0]);

  for (let i = 0; i < facepicTotalAttachments.length; i += 1) {
    await postgreSQL`INSERT INTO public."facepicAttachment" ${postgreSQL(facepicTotalAttachments[i], facepicAttachmentColumns)}`;
    console.log(`=== Inserted ${facepicTotalAttachments[i].length} facepicAttachments ===`);
  }

  ordinaryTotalAttachments.push(ordinaryAttachments);
  const ordinaryAttachmentColumns = Object.keys(ordinaryTotalAttachments[0][0]);

  for (let i = 0; i < ordinaryTotalAttachments.length; i += 1) {
    await postgreSQL`INSERT INTO public."ordinaryAttachment" ${postgreSQL(ordinaryTotalAttachments[i], ordinaryAttachmentColumns)}`;
    console.log(`=== Inserted ${ordinaryTotalAttachments[i].length} ordinaryAttachments ===`);
  }
};

export {importAttachments};
