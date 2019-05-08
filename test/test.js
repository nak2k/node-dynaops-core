const test = require('tape');
const { syncTables } = require('..');
const AWS = require('aws-sdk');

const dynamodb = new AWS.DynamoDB();

test('test syncTables', t => {
  t.plan(1);

  const options = {
    dynamodb,
    tableDefs: {
      Globals: {
        PTIR: true,
      },
      Attributes: {
        path: { Type: 'S' },
        created: { Type: 'N' },
        modified: { Type: 'N' },
      },
      Tables: [
        {
          TableName: 'dynaops.Test',
          PartitionKey: 'path',
          SortKey: 'created',
          LocalSecondaryIndexes: [
            {
              IndexName: 'Modified',
              SortKey: 'modified',
              Projection: [ 'title', 'created' ],
            },
          ],
        },
      ],
    },
    verbose: true,
  };

  syncTables(options, (err, result) => {
    t.error(err);
  });
});
