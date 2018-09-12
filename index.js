/**
 * Delete all objects and versions under a given bucket and prefix.
 * Providing access key ID and secret access key manually is required;
 * you should create a new user for each delete operation and apply
 * appropriate permissions so you don't risk deleting anything you don't
 * want to.
 */

const AWS = require('aws-sdk');
const commander = require('commander');

commander
  .version('0.1.0', '-v, --version')
  .option('--bucket <bucket>', 'S3 bucket name')
  .option('--prefix <prefix>', 'Object prefix')
  .option('--access-key-id <accessKeyId>', 'AWS access key ID')
  .option('--secret-access-key <secretAccessKey>', 'AWS secret access key')
  .parse(process.argv);

if (!commander.bucket) {
  console.log('Bucket name is required');
  process.exit(1);
}

if (!commander.accessKeyId || !commander.secretAccessKey) {
  console.log('You must specify AWS credentials');
  process.exit(1);
}

if (commander.prefix) {
  console.log(`rm s3://${commander.bucket}/${commander.prefix}`);
} else {
  console.log(`rm s3://${commander.bucket}/*`);
}

function rm(bucket, prefix, nextKeyMarker, nextVersionIdMarker, callback) {
  var s3 = new AWS.S3({
    accessKeyId: commander.accessKeyId,
    secretAccessKey: commander.secretAccessKey
  });

  s3.listObjectVersions({
    Bucket: bucket,
    Prefix: prefix,
    KeyMarker: nextKeyMarker,
    VersionIdMarker: nextVersionIdMarker,
    MaxKeys: 1000
  }, (err, data) => {
    if (err) {
      console.log(`error: ${err}`);
      process.exit(1);
    }

    var done = !data.IsTruncated;
    var nextKeyMarker = data.NextKeyMarker;
    var nextVersionIdMarker = data.NextVersionIdMarker;

    var versions = data.Versions.map((ver) => {
      return {
        Key: ver.Key,
        VersionId: ver.VersionId
      };
    });

    var deleteMarkers = data.DeleteMarkers.map((marker) => {
      return {
        Key: marker.Key,
        VersionId: marker.VersionId
      };
    });

    var objectList = versions.concat(deleteMarkers);

    if (objectList.length === 0) {
      return callback();
    }

    console.log(objectList);

    s3.deleteObjects({
      Bucket: bucket,
      Delete: {
        Objects: objectList,
        Quiet: true
      }
    }, (err) =>  {
      if (err) {
        return callback(err);
      }

      if (done) {
        return callback();
      } else {
        return rm(bucket, prefix, nextKeyMarker, nextVersionIdMarker, callback);
      }
    });
  });
}

rm(commander.bucket, commander.prefix, null, null, (err) => {
  if (err) {
    console.log(err);
    process.exit(1);
  }

  console.log('done');
  process.exit(0);
});
