const BaseQuery = require('@cubejs-backend/schema-compiler/adapter/BaseQuery');

const GRANULARITY_TO_INTERVAL = {
  day: 'DD',
  week: 'W',
  hour: 'HH24',
  minute: 'mm',
  second: 'ss',
  month: 'MM',
  year: 'YY'
};

class AzureTablesQuery extends BaseQuery {
  convertTz(field) {
    return `${field}`;
  }

  // // eslint-disable-next-line no-unused-vars
  // timeStampParam(timeDimension) {
  //   return this.timeStampCast(`?`);
  // }

  // timeGroupedColumn(granularity, dimension) {
  //   return `${dimension}__DAY`;
  // }

  // escapeColumnName(name) {
  //   return `${name}`;
  // }
}

module.exports = AzureTablesQuery;
