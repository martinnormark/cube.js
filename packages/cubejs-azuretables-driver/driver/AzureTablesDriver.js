const { BaseDriver } = require('@cubejs-backend/query-orchestrator');
const AzureTablesQuery = require('./AzureTablesQuery');

const azure = require('azure-storage');
const { Parser } = require('node-sql-parser');

const GenericTypeToAzureTables = {
  string: azure.TableUtilities.EdmType.STRING,
  text: azure.TableUtilities.EdmType.STRING,
  int: azure.TableUtilities.EdmType.INT32,
  bigint: azure.TableUtilities.EdmType.INT64,
  datetime: azure.TableUtilities.EdmType.DATETIME,
  date: azure.TableUtilities.EdmType.DATETIME,
  boolean: azure.TableUtilities.EdmType.BOOLEAN
};

const DEFAULT_SCHEMA = 'DEFAULT_SCHEMA';
const parser = new Parser();

class AzureTablesDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      storageAccount: process.env.CUBEJS_DB_NAME,
      storageAccessKey: process.env.CUBEJS_DB_PASS,
      host: process.env.CUBEJS_DB_HOST && process.env.CUBEJS_DB_HOST.trim().length > 0 ?
        process.env.CUBEJS_DB_HOST : undefined,
      requestTimeout: 10 * 60 * 1000, // 10 minutes
      options: {
        useUTC: false
      },
      ...config
    };

    this.tableServicePromise = Promise.resolve(azure.createTableService(this.config.storageAccount, this.config.storageAccessKey, this.config.host));
  }

  static dialectClass() {
    return AzureTablesQuery;
  }

  static driverEnvVariables() {
    return [
      'CUBEJS_DB_NAME', 'CUBEJS_DB_PASS', 'CUBEJS_DB_HOST'
    ];
  }

  informationSchemaQuery() {
    return 'INFORMATION_SCHEMA';
  }

  testConnection() {
    return new Promise((resolve, reject) => {
      this.tableServicePromise.then(tblSvc => tblSvc.listTablesSegmented(null, null, (err, res) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(res != null);
        }
      }));
    });
  }

  query(query, values) {
    console.log('querying', query, values);

    if (query === this.informationSchemaQuery())
      return this.getInformationSchema();

    if (query === this.getTablesQuery())
      return this.getTables();

    // Handle cube.js special query, by naively returning the value expected.
    if (query?.startsWith('SELECT FLOOR((EXTRACT')) {
      return Promise.resolve([Math.floor(Date.now() / 10)]);
    }

    if (query?.startsWith('SELECT')) {
      try {
        const { tableList, columnList, ast } = parser.parse(query);
        console.log('parsed SQL', tableList, columnList, ast);
      }
      catch (err) {
        console.error(err);
      }
      finally {

      }

      return Promise.resolve([]);
    }

    return new Promise((resolve, reject) => {
      this.tableServicePromise.then((tblSvc) => {

        const table = values.shift();
        let tableQuery = new azure.TableQuery()
          .top(5);

        if (query) {
          tableQuery = tableQuery.where(query, ...values);
        }

        tblSvc.queryEntities(table, tableQuery, null, null, (err, res) => {
          if (err) {
            reject(err);
          }
          else {
            resolve(res.entries);
          }
        });
      });
    });
  }

  getInformationSchema() {
    return new Promise((resolve, reject) => {
      this.getTables().then(tables => {
        const proms = [];

        tables.forEach(tableName => {
          proms.push(
            this.query(null, [tableName]).then(tableRow => {
              const infoSchema = [];

              for (const key in tableRow[0]) {
                if (key[0] !== '.' && tableRow[0].hasOwnProperty(key)) {
                  const column = tableRow[0][key];

                  infoSchema.push({
                    'column_name': key,
                    'table_name': tableName,
                    'table_schema': DEFAULT_SCHEMA,
                    'data_type': this.toGenericType(column['$']) || 'text'
                  });
                }
              }

              return infoSchema;
            })
          );
        });

        Promise.all(proms).then(schemas => resolve(schemas.flat(1)));
      });
    });
  }

  getTablesQuery(schemaName) {
    return "GET_TABLES";
  }

  getTables() {
    return new Promise((resolve, reject) => {
      this.tableServicePromise.then(tblSvc => tblSvc.listTablesSegmented(null, null, (err, res) => {
        if (err) {
          reject(err);
        }
        else {
          resolve(res.entries);
        }
      }));
    });
  }

  getSasToken(tableName) {
    var sharedAccessPolicy = {
      AccessPolicy: {
        Permissions: azure.TableUtilities.SharedAccessPermissions.QUERY,
        Expiry: new Date('October 12, 2021 11:53:40 am GMT'),
        Protocols: 'https'
      }
    };

    return this.tableServicePromise.then(tblSvc => {
      const sasToken = tblSvc.generateSharedAccessSignature(tableName, sharedAccessPolicy);
      return tblSvc.getUrl(tableName, sasToken);
    });
  }

  createSchemaIfNotExists(schemaName) {
    return Promise.resolve(null);
  }

  async downloadQueryResults(query, values) {
    const result = await this.query(query, values);
    const types = Object.keys(result.columns).map((key) => ({
      name: result.columns[key].name,
      type: this.toGenericType(result.columns[key].type.declaration),
    }));

    return {
      rows: result,
      types,
    };
  }

  readOnly() {
    return !!this.config.readOnly;
  }

  toGenericType(columnType) {
    for (const key in GenericTypeToAzureTables) {
      if (GenericTypeToAzureTables.hasOwnProperty(key)) {
        const element = GenericTypeToAzureTables[key];
        
        if (element === columnType)
          return key;
      }
    }

    return columnType;
  }

  fromGenericType(columnType) {
      return GenericTypeToAzureTables[columnType.toLowerCase()] || columnType;
  }
}

module.exports = AzureTablesDriver;
