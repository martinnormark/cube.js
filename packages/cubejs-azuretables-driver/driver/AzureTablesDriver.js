const { BaseDriver } = require('@cubejs-backend/query-orchestrator');
const AzureTablesQuery = require('./AzureTablesQuery');

const azure = require('azure-storage');

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
    return 'GIMME_YULE';
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

    if (query === this.informationSchemaQuery()) {
      return Promise.resolve([
        { 'column_name': 'prid', 'table_name': 'PullRequests', 'table_schema': 'YULE', 'data_type': 'text' },
        { 'column_name': 'commits', 'table_name': 'PullRequests', 'table_schema': 'YULE', 'data_type': 'text' },
        { 'column_name': 'title', 'table_name': 'PullRequests', 'table_schema': 'YULE', 'data_type': 'text' },
        { 'column_name': 'org', 'table_name': 'PullRequests', 'table_schema': 'YULE', 'data_type': 'text' },
        { 'column_name': 'author', 'table_name': 'PullRequests', 'table_schema': 'YULE', 'data_type': 'text' },
        { 'column_name': 'cid', 'table_name': 'PullRequestCommits', 'table_schema': 'YULE', 'data_type': 'text' },
        { 'column_name': 'adds', 'table_name': 'PullRequestCommits', 'table_schema': 'YULE', 'data_type': 'text' },
      ]);
    }

    return new Promise((resolve, reject) => {
      this.tableServicePromise.then((tblSvc) => {

        const table = values.shift();
        const tableQuery = new azure.TableQuery()
          .top(5)
          .where(query, ...values);

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

  getTablesQuery(schemaName) {
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
}

module.exports = AzureTablesDriver;
