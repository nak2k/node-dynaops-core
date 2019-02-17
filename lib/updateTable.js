const { removeUndefined } = require('./util');

function updateTable(options, tableDef, currentTable, callback) {
  /*
   * Check differences.
   */
  const params = {
    TableName: tableDef.TableName,
  };

  return callback(null);

  /*
   * Update.
   */
  function invoke() {
    options.dynamodb.updateTable(params, (err, data) => {
      if (err) {
        if (err.code !== 'LimitExceededException' && err.code !== 'ResourceInUseException') {
          return callback(err);
        }

        setTimeout(updateTable, 30000, options, tableDef, currentTable, callback);
        return;
      }

      updateTable(options, tableDef, data.TableDescription, callback);
    });
  }
}

exports.updateTable = updateTable;
