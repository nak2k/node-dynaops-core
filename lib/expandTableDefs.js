const each = require('async-each');
const parallel = require('run-parallel');

function expandTableDefs(tableDefs, callback) {
  const {
    Attributes = {},
    Tables = [],
  } = tableDefs;

  const context = {
    Attributes,
  };

  each(Tables, expandTableDef.bind(null, context), (err, Tables) => {
    if (err) {
      return callback(err);
    }

    callback(null, {
      Tables,
    });
  });
}

function expandTableDef(context, table, callback) {
  if (typeof table !== 'object') {
    return callback(new Error(`All elements of Tables must be an object`));
  }

  const tableContext = {
    ...context,
    AttributeDefinitions: [...table.AttributeDefinitions || []],
  };

  expandKeySchema(tableContext, table, (err, KeySchema) => {
    if (err) {
      return callback(err);
    }

    const {
      LocalSecondaryIndexes,
      GlobalSecondaryIndexes,
    } = table;

    parallel({
      LocalSecondaryIndexes: callback =>
        expandSecondaryIndexes({
          ...tableContext,
          PartitionKey: KeySchema[0],
        }, LocalSecondaryIndexes, callback),
      GlobalSecondaryIndexes: callback =>
        expandSecondaryIndexes(tableContext, GlobalSecondaryIndexes, callback),
    }, (err, result) => {
      if (err) {
        return callback(err);
      }

      const {
        TableName,
        ProvisionedThroughput,
        StreamSpecification,
        SSESpecification,
      } = table;

      callback(null, {
        TableName,
        AttributeDefinitions: tableContext.AttributeDefinitions,
        KeySchema,
        BillingMode: ProvisionedThroughput ? 'PROVISIONED' : 'PAY_PER_REQUEST',
        ProvisionedThroughput,
        StreamSpecification,
        SSESpecification,
        ...result,
      });
    });
  });
}

function expandSecondaryIndexes(context, indexes, callback) {
  if (!indexes) {
    return callback(null, undefined);
  }

  const {
    PartitionKey,
  } = context;

  each(indexes, (index, callback) => {
    expandKeySchema(context, index, (err, KeySchema) => {
      if (err) {
        return callback(err);
      }

      const {
        IndexName,
        Projection,
      } = index;

      let expandedProjection;

      if (typeof Projection === 'string') {
        expandedProjection = {
          ProjectionType: Projection,
        };
      } else if (Array.isArray(Projection)) {
        expandedProjection = {
          ProjectionType: 'INCLUDE',
          NonKeyAttributes: Projection,
        };
      } else if (typeof Projection === 'object') {
        expandedProjection = Projection;
      } else {
        return callback(null, `'Projection' must be either a string or an array or an object`);
      }

      if (PartitionKey) {
        KeySchema[0] = PartitionKey;

        callback(null, {
          IndexName,
          KeySchema,
          Projection: expandedProjection,
        });
      } else {
        callback(null, {
          IndexName,
          KeySchema,
          Projection: expandedProjection,
          ProvisionedThroughput: index.ProvisionedThroughput,
        });
      }
    });
  }, callback);
}

function expandKeySchema(context, tableOrIndex, callback) {
  const { Attributes, AttributeDefinitions } = context;

  const KeySchema = [...tableOrIndex.KeySchema || []];

  const {
    PartitionKey,
    SortKey,
  } = tableOrIndex;

  if (PartitionKey) {
    KeySchema[0] = {
      AttributeName: PartitionKey,
      KeyType: 'HASH',
    };

    const attr = Attributes[PartitionKey];

    if (!attr) {
      return callback(new Error(`Attribute '${PartitionKey}' not defined in Attributes`));
    }

    AttributeDefinitions.push({
      AttributeName: PartitionKey,
      AttributeType: attr.Type,
    });
  }

  if (SortKey) {
    KeySchema[1] = {
      AttributeName: SortKey,
      KeyType: 'RANGE',
    };

    const attr = Attributes[SortKey];

    if (!attr) {
      return callback(new Error(`Attribute '${SortKey}' not defined in Attributes`));
    }

    AttributeDefinitions.push({
      AttributeName: SortKey,
      AttributeType: attr.Type,
    });
  }

  callback(null, KeySchema);
}

exports.expandTableDefs = expandTableDefs;
