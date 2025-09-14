const { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand, CopyObjectCommand, DeleteObjectCommand } = require("@aws-sdk/client-s3");
const csv = require('csv-parser');
const s3 = new S3Client({});

exports.handler = async (event) => {
  const bucket = 'splitterbucket';
  const inboxPrefix = 'inbox/';
  const splitPrefix = 'split/';
  const processedPrefix = 'processed/';

  const listParams = {
    Bucket: bucket,
    Prefix: inboxPrefix,
  };

  try {
    const listedObjects = await s3.send(new ListObjectsV2Command(listParams));

    if (!listedObjects.Contents || listedObjects.Contents.length === 0) {
      console.log('Inbox is empty. Nothing to process.');
      return 'Processing complete. No files found.';
    }

    for (const object of listedObjects.Contents) {
      const key = object.Key;
      if (key === inboxPrefix) continue; // Skip the directory itself

      const records = [];
      let header = '';

      const getObjectCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
      const s3Stream = (await s3.send(getObjectCommand)).Body;

      await new Promise((resolve, reject) => {
        s3Stream
          .pipe(csv())
          .on('headers', (headers) => {
            header = headers.join(',') + '\n';
          })
          .on('data', (data) => {
            records.push(data);
          })
          .on('end', async () => {
            const chunkSize = 75000;
            for (let i = 0; i < records.length; i += chunkSize) {
              const chunk = records.slice(i, i + chunkSize);
              const chunkCsv = chunk.map(row => Object.values(row).join(',')).join('\n');
              const newKey = `${processedPrefix}${key.split('/').pop().split('.')[0]}-chunk-${i / chunkSize + 1}.csv`;

              const uploadParams = {
                Bucket: bucket,
                Key: newKey,
                Body: header + chunkCsv,
              };

              try {
                await s3.send(new PutObjectCommand(uploadParams));
                console.log(`Successfully uploaded ${newKey}`);
              } catch (err) {
                console.error(`Error uploading ${newKey}:`, err);
                reject(err);
                return;
              }
            }

            const destKey = `${splitPrefix}${key.substring(inboxPrefix.length)}`;
            const copyParams = {
              Bucket: bucket,
              CopySource: `${bucket}/${key}`,
              Key: destKey,
            };

            try {
              await s3.send(new CopyObjectCommand(copyParams));
              await s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
              console.log(`Successfully moved ${key} to ${destKey}`);
            } catch (err) {
              console.error(`Error moving ${key}:`, err);
              reject(err);
              return;
            }
            resolve();
          })
          .on('error', (err) => {
            reject(err);
          });
      });
    }
    return 'Processing complete.';
  } catch (err) {
    console.error('Error listing objects in inbox:', err);
    throw err;
  }
};