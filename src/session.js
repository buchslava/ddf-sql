const fs = require('fs');
const path = require('path');
const { Parser } = require('flora-sql-parser');
const { promisify } = require('util');
const { intersection, difference, isEmpty, includes } = require('lodash');
const getRecordFilterFun = require('./filter');
const query = require('./query');
const foo = require('./optimize');
const readFile = promisify(fs.readFile);

module.exports = class Session {
  constructor(basePath) {
    this.basePath = basePath;
    this.datpackagePath = path.resolve(basePath, 'datapackage.json');
  }

  async runSQL(sqlQuery) {
    if (!this.datapackage) {
      this.datapackage = JSON.parse(await readFile(this.datpackagePath, 'utf-8'));
      // new
      /*this.resourcesHash = this.datapackage.resources.reduce((hash, resource) => {
        hash[resource.name] = resource.path;
        return hash;
      }, {});*/
    }

    const parser = new Parser();
    const ast = parser.parse(sqlQuery);
    const columnNames = ast.columns.map(columnDesc => columnDesc.expr.column);
    const resourcesMap = new Map();

    // new
    await foo(ast.where, this.datapackage);
    // change one generic key to many particular

    for (const conceptDesc of this.datapackage.ddfSchema[ast.from[0].table]) {
      const keys = intersection(conceptDesc.primaryKey, columnNames);
      const values = difference(columnNames, conceptDesc.primaryKey);

      if (!isEmpty(keys) && includes(values, conceptDesc.value)) {
        for (const resource of this.datapackage.resources) {
          if (includes(conceptDesc.resources, resource.name)) {
            if (!resourcesMap.has(resource.path)) {
              resourcesMap.set(resource.path, { keys, values: new Set() });
            }

            const resourceDesc = resourcesMap.get(resource.path);
            resourceDesc.values.add(conceptDesc.value);

            resourcesMap.set(resource.path, resourceDesc);
          }
        }
      }
    }

    console.log(resourcesMap);
    process.exit(0);


    const recordFilterFun = getRecordFilterFun(sqlQuery, ast);
    return await query(this.basePath, resourcesMap, recordFilterFun);
  }
}
