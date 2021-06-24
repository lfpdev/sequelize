'use strict';

const Utils = require('../../utils');
const AbstractQuery = require('../abstract/query');
const sequelizeErrors = require('../../errors');
const _ = require('lodash');
const { logger } = require('../../utils/logger');

const debug = logger.debugContext('sql:mysql');


class Query extends AbstractQuery {
  constructor(connection, sequelize, options) {
    super(connection, sequelize, Object.assign({ showWarnings: false }, options));
  }

  static formatBindParameters(sql, values, dialect) {
    const bindParam = [];
    const replacementFunc = (match, key, values) => {
      if (values[key] !== undefined) {
        bindParam.push(values[key]);
        return '?';
      }
      return undefined;
    };
    sql = AbstractQuery.formatBindParameters(sql, values, dialect, replacementFunc)[0];
    return [sql, bindParam.length > 0 ? bindParam : undefined];
  }

  run(sql, parameters) {
    this.sql = sql;
    const { connection, options } = this;

    //do we need benchmark for this query execution
    const showWarnings = this.sequelize.options.showWarnings || options.showWarnings;

    const complete = this._logQuery(sql, debug, parameters);

    return new Utils.Promise((resolve, reject) => {
      const handler = (err, results) => { // 传一个 callback 到 mysql2
        console.log('[sequelize] >>>>>> 调用 handler err = ', err);
        console.log('[sequelize] >>>>>> 调用 handler results = ', results);
        complete();

        if (err) {
          // MySQL automatically rolls-back transactions in the event of a deadlock
          if (options.transaction && err.errno === 1213) {
            options.transaction.finished = 'rollback';
          }
          err.sql = sql;
          err.parameters = parameters;

          reject(this.formatError(err));
        } else {
          resolve(results);
        }
      };
      if (parameters) {
        debug('parameters(%j)', parameters);
        connection.execute(sql, parameters, handler).setMaxListeners(100); // execute　就是　mysql2 connection.js 中的方法 L564
      } else {
        connection.query({ sql }, handler).setMaxListeners(100);　//　query　是 mysql2 connection.js 中的方法 L509
      }
    })
      // Log warnings if we've got them.
      .then(results => {
        console.log('[sequelize] query run then1111111111111111111 ', results);
        if (showWarnings && results && results.warningStatus > 0) {
          return this.logWarnings(results);
        }
        return results;
      })
      // Return formatted results...
      .then(results => {
        console.log('[sequelize] query run then22222222222222222222', results);
        const res = this.formatResults(results) // 对于 insert 操作，mysql2 返回结果中不包含插入的数据，sequelize.create 返回的model是自己保存的一份，时间为 UTC 格式
        console.log('[sequelize] query run then33333333333333333333', res);
        return res;
      });
  }

  /**
   * High level function that handles the results of a query execution.
   *
   *
   * Example:
   *  query.formatResults([
   *    {
   *      id: 1,              // this is from the main table
   *      attr2: 'snafu',     // this is from the main table
   *      Tasks.id: 1,        // this is from the associated table
   *      Tasks.title: 'task' // this is from the associated table
   *    }
   *  ])
   *
   * @param {Array} data - The result of the query execution.
   * @private
   */
  formatResults(data) {
    let result = this.instance;

    if (this.isInsertQuery(data)) {
      this.handleInsertQuery(data);

      if (!this.instance) {
        // handle bulkCreate AI primiary key
        if (
          data.constructor.name === 'ResultSetHeader'
          && this.model
          && this.model.autoIncrementAttribute
          && this.model.autoIncrementAttribute === this.model.primaryKeyAttribute
          && this.model.rawAttributes[this.model.primaryKeyAttribute]
        ) {
          const startId = data[this.getInsertIdField()];
          result = [];
          for (let i = startId; i < startId + data.affectedRows; i++) {
            result.push({ [this.model.rawAttributes[this.model.primaryKeyAttribute].field]: i });
          }
        } else {
          result = data[this.getInsertIdField()];
        }
      }
    }

    if (this.isSelectQuery()) {
      console.trace('[sequelize] 包装最后结果1===========');
      return this.handleSelectQuery(data);
    }
    if (this.isShowTablesQuery()) {
      console.trace('[sequelize] 包装最后结果2===========');
      return this.handleShowTablesQuery(data);
    }
    if (this.isDescribeQuery()) {
      result = {};

      for (const _result of data) {
        const enumRegex = /^enum/i;
        result[_result.Field] = {
          type: enumRegex.test(_result.Type) ? _result.Type.replace(enumRegex, 'ENUM') : _result.Type.toUpperCase(),
          allowNull: _result.Null === 'YES',
          defaultValue: _result.Default,
          primaryKey: _result.Key === 'PRI',
          autoIncrement: Object.prototype.hasOwnProperty.call(_result, 'Extra') && _result.Extra.toLowerCase() === 'auto_increment',
          comment: _result.Comment ? _result.Comment : null
        };
      }
      console.trace('[sequelize] 包装最后结果3===========');
      return result;
    }
    if (this.isShowIndexesQuery()) {
      console.trace('[sequelize] 包装最后结果4===========');
      return this.handleShowIndexesQuery(data);
    }
    if (this.isCallQuery()) {
      console.trace('[sequelize] 包装最后结果5===========');
      return data[0];
    }
    if (this.isBulkUpdateQuery() || this.isBulkDeleteQuery() || this.isUpsertQuery()) {
      console.trace('[sequelize] 包装最后结果6===========');
      return data.affectedRows;
    }
    if (this.isVersionQuery()) {
      console.trace('[sequelize] 包装最后结果7===========');
      return data[0].version;
    }
    if (this.isForeignKeysQuery()) {
      console.trace('[sequelize] 包装最后结果8===========');
      return data;
    }
    if (this.isInsertQuery() || this.isUpdateQuery()) {
      console.trace('[sequelize] 包装最后结果9===========');
      return [result, data.affectedRows];
    }
    if (this.isShowConstraintsQuery()) {
      console.trace('[sequelize] 包装最后结果10===========');
      return data;
    }
    if (this.isRawQuery()) {
      // MySQL returns row data and metadata (affected rows etc) in a single object - let's standarize it, sorta
      console.trace('[sequelize] 包装最后结果11===========');
      return [data, data];
    }
    console.trace('[sequelize] 包装最后结果12===========');
    return result;
  }

  logWarnings(results) {
    return this.run('SHOW WARNINGS').then(warningResults => {
      const warningMessage = `MySQL Warnings (${this.connection.uuid || 'default'}): `;
      const messages = [];
      for (const _warningRow of warningResults) {
        if (_warningRow === undefined || typeof _warningRow[Symbol.iterator] !== 'function') continue;
        for (const _warningResult of _warningRow) {
          if (Object.prototype.hasOwnProperty.call(_warningResult, 'Message')) {
            messages.push(_warningResult.Message);
          } else {
            for (const _objectKey of _warningResult.keys()) {
              messages.push([_objectKey, _warningResult[_objectKey]].join(': '));
            }
          }
        }
      }

      this.sequelize.log(warningMessage + messages.join('; '), this.options);

      return results;
    });
  }

  formatError(err) {
    const errCode = err.errno || err.code;

    switch (errCode) {
      case 1062: {
        const match = err.message.match(/Duplicate entry '([\s\S]*)' for key '?((.|\s)*?)'?$/);
        let fields = {};
        let message = 'Validation error';
        const values = match ? match[1].split('-') : undefined;
        const fieldKey = match ? match[2] : undefined;
        const fieldVal = match ? match[1] : undefined;
        const uniqueKey = this.model && this.model.uniqueKeys[fieldKey];

        if (uniqueKey) {
          if (uniqueKey.msg) message = uniqueKey.msg;
          fields = _.zipObject(uniqueKey.fields, values);
        } else {
          fields[fieldKey] = fieldVal;
        }

        const errors = [];
        _.forOwn(fields, (value, field) => {
          errors.push(new sequelizeErrors.ValidationErrorItem(
            this.getUniqueConstraintErrorMessage(field),
            'unique violation', // sequelizeErrors.ValidationErrorItem.Origins.DB,
            field,
            value,
            this.instance,
            'not_unique'
          ));
        });

        return new sequelizeErrors.UniqueConstraintError({ message, errors, parent: err, fields });
      }

      case 1451:
      case 1452: {
        // e.g. CONSTRAINT `example_constraint_name` FOREIGN KEY (`example_id`) REFERENCES `examples` (`id`)
        const match = err.message.match(/CONSTRAINT ([`"])(.*)\1 FOREIGN KEY \(\1(.*)\1\) REFERENCES \1(.*)\1 \(\1(.*)\1\)/);
        const quoteChar = match ? match[1] : '`';
        const fields = match ? match[3].split(new RegExp(`${quoteChar}, *${quoteChar}`)) : undefined;

        return new sequelizeErrors.ForeignKeyConstraintError({
          reltype: String(errCode) === '1451' ? 'parent' : 'child',
          table: match ? match[4] : undefined,
          fields,
          value: fields && fields.length && this.instance && this.instance[fields[0]] || undefined,
          index: match ? match[2] : undefined,
          parent: err
        });
      }

      default:
        return new sequelizeErrors.DatabaseError(err);
    }
  }

  handleShowIndexesQuery(data) {
    // Group by index name, and collect all fields
    data = data.reduce((acc, item) => {
      if (!(item.Key_name in acc)) {
        acc[item.Key_name] = item;
        item.fields = [];
      }

      acc[item.Key_name].fields[item.Seq_in_index - 1] = {
        attribute: item.Column_name,
        length: item.Sub_part || undefined,
        order: item.Collation === 'A' ? 'ASC' : undefined
      };
      delete item.column_name;

      return acc;
    }, {});

    return _.map(data, item => ({
      primary: item.Key_name === 'PRIMARY',
      fields: item.fields,
      name: item.Key_name,
      tableName: item.Table,
      unique: item.Non_unique !== 1,
      type: item.Index_type
    }));
  }
}

module.exports = Query;
module.exports.Query = Query;
module.exports.default = Query;
