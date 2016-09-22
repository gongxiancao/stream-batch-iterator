module.exports = function (stream, filter, iterator, options) {
  var batch = [];
  var batchSize = options.batchSize;
  return new Promise(function (resolve, reject) {
    stream.on('data', function (doc) {
      if(filter && !filter(doc)) {
        return;
      }
      batch.push(doc);
      if(batch.length < batchSize) {
        return;
      }
      stream.pause();
      var commit = batch;
      batch = [];

      Promise.map(commit, iterator, {concurrency: options.concurrency})
        .then(function () {
          stream.resume();
          return null;
        })
        .catch(function (err) {
          stream.destroy(err);
          reject(err);
        });
      return null;
    })
    .on('error', reject)
    .on('close', function () {
      resolve();
    });
  })
  .then(function () {
    return Promise.map(batch, iterator, {concurrency: options.concurrency});
  });
};