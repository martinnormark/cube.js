const { BaseDriver } = require('@cubejs-backend/query-orchestrator');

class AzureTablesDriver extends BaseDriver {
  constructor(config) {
    super();
    this.config = {
      server: process.env.CUBEJS_DB_HOST, // windows.net
      database: process.env.CUBEJS_DB_NAME, // account
      password: process.env.CUBEJS_DB_PASS, // key
      domain: process.env.CUBEJS_DB_DOMAIN && process.env.CUBEJS_DB_DOMAIN.trim().length > 0 ?
        process.env.CUBEJS_DB_DOMAIN : undefined, // CosmosDB endpoint
      requestTimeout: 10 * 60 * 1000, // 10 minutes
      options: {
        useUTC: false
      },
      ...config
    };
    this.config = config;
  }

  static driverEnvVariables() {
    return [
      'CUBEJS_DB_HOST', 'CUBEJS_DB_NAME', 'CUBEJS_DB_PASS', 'CUBEJS_DB_DOMAIN'
    ];
  }

  testConnection() {
    return this.initialConnectPromise.then((pool) => pool.request().query('SELECT 1 as number'));
  }

  query(query, values) {
    let cancelFn = null;
    const promise = this.initialConnectPromise.then((pool) => {
      const request = pool.request();
      (values || []).forEach((v, i) => request.input(`_${i + 1}`, v));

      // TODO time zone UTC set in driver ?

      cancelFn = () => request.cancel();
      return request.query(query).then(res => res.recordset);
    });
    promise.cancel = () => cancelFn && cancelFn();
    return promise;
  }

  param(paramIndex) {
    return `@_${paramIndex + 1}`;
  }

  createSchemaIfNotExists(schemaName) {
    return this.query(
      `SELECT schema_name FROM information_schema.schemata WHERE schema_name = ${this.param(0)}`,
      [schemaName]
    ).then((schemas) => {
      if (schemas.length === 0) {
        return this.query(`CREATE SCHEMA ${schemaName}`);
      }
      return null;
    });
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

module.exports = MSSqlDriver;
