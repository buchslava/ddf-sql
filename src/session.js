const fs = require('fs');
const path = require('path');
const { Parser, util } = require('flora-sql-parser');
const { promisify } = require('util');
const { intersection, difference, isEmpty, includes, clone } = require('lodash');
const getRecordFilterFun = require('./filter');
const query = require('./query');
const getConceptsInfo = require('./concepts');
const optimizator = require('./optimize');
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
    const columnNamesTemplate = ast.columns.map(columnDesc => columnDesc.expr.column);
    const resourcesMap = new Map();

    // //////////////////////////////////////////////////////
    const { conceptTypeHash } = await getConceptsInfo(this.basePath, this.datapackage);
    const { enityConditionDescs } = optimizator(ast.where, this.datapackage);
    const columnNamesCompletes = [];

    for (const desc of enityConditionDescs) {
      const oldKey = desc.entity;
      const newKey = desc.property.substr(4);
      const newNamesComplete = clone(columnNamesTemplate);
      const index = newNamesComplete.indexOf(oldKey);
      newNamesComplete.splice(index, 1, newKey);
      columnNamesCompletes.push(newNamesComplete);
    }

    if (isEmpty(columnNamesCompletes)) {
      columnNamesCompletes.push(columnNamesTemplate);
    }

    // console.log(util.astToSQL(ast));

    for (const conceptDesc of this.datapackage.ddfSchema[ast.from[0].table]) {
      for (const columnNames of columnNamesCompletes) {
        const keys = intersection(conceptDesc.primaryKey, columnNames);
        const values = difference(columnNames, conceptDesc.primaryKey);

        if (keys.length === conceptDesc.primaryKey.length &&
          conceptDesc.primaryKey.length + values.length === columnNames.length &&
          this.notEntities(values, conceptTypeHash)) {
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
    }

    // process.exit(0);

    const recordFilterFun = getRecordFilterFun(ast);
    return await query(this.basePath, resourcesMap, recordFilterFun);
  }

  notEntities(values, conceptTypeHash) {
    for (const value of values) {
      if (conceptTypeHash[value] === 'entity_set' || conceptTypeHash[value] === 'entity_domain') {
        return false;
      }
    }

    return true;
  }
}
