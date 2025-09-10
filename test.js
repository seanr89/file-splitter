const handler = require('./index').handler;

const event = {
  Records: [
    {
      s3: {
        bucket: {
          name: 'your-s3-bucket-name',
        },
        object: {
          key: 'your-s3-object-key.csv',
        },
      },
    },
  ],
};

handler(event)
  .then(message => console.log(message))
  .catch(err => console.error(err));
