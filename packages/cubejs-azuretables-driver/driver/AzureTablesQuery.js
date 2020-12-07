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

  timeStampParam(timeDimension) {
    return this.timeStampCast(`?`);
  }

  timeStampCast(value) {
    return `${value}`;
  }

  dateTimeCast(value) {
    return `${value}`;
  }

  subtractInterval(date, interval) {
    return `${date}`;
  }

  addInterval(date, interval) {
    return `${date}`;
  }

  timeGroupedColumn(granularity, dimension) {
    return `${dimension}`;
  }

  escapeColumnName(name) {
    return `${name}`;
  }
}

module.exports = AzureTablesQuery;
