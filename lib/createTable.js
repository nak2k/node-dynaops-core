const { removeUndefined } = require('./util');

function createTable(options, tableDef, callback) {
  options.dynamodb.createTable(removeUndefined(tableDef), (err, data) => {
    if (err) {
      if (err.code !== 'LimitExceededException') {
        return callback(err);
      }

      setTimeout(createTable, 30000, options, tableDef, callback);
      return;
    }

    callback(null, data.TableDescription);
  });
}

exports.createTable = createTable;
