const { expandTableDefs } = require('./expandTableDefs');
const { createTable } = require('./createTable');
const { updateTable } = require('./updateTable');
const each = require('async-each');

function syncTables(options, callback) {
  const {
    tableDefs,
    verbose,
  } = options;

  expandTableDefs(tableDefs, (err, tableDefs) => {
    if (err) {
      return callback(err);
    }

    if (verbose) {
      console.log('# Expanded Table Definitions\n');
      console.dir(tableDefs, { depth: null });
      console.log();
    }

    each(tableDefs.Tables, syncTable.bind(null, options), (err, result) => {
      if (err) {
        return callback(err);
      }

      if (verbose) {
        console.log('# Result\n');
        console.dir(result, { depth: null });
        console.log();
      }

      callback(null, result);
    });
  });
}

function syncTable(options, tableDef, callback) {
  const params = {
    TableName: tableDef.TableName,
  };

  options.dynamodb.describeTable(params, (err, data) => {
    if (err) {
      if (err.code !== 'ResourceNotFoundException') {
        return callback(err);
      }

      createTable(options, tableDef, callback);
    } else {
      updateTable(options, tableDef, data.Table, callback);
    }
  });
}

exports.syncTables = syncTables;
