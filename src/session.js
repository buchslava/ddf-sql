const fs = require('fs');
const path = require('path');
const { Parser, util } = require('flora-sql-parser');
const { promisify } = require('util');
const { intersection, difference, isEmpty, includes, clone } = require('lodash');
const getRecordFilterFun = require('./filter');
const query = require('./query');
const getConceptsInfo = require('./concepts');
const optimizator = require('./optimizator');
const readFile = promisify(fs.readFile);

module.exports = class Session {
  constructor(basePath) {
    this.basePath = basePath;
    this.datpackagePath = path.resolve(basePath, 'datapackage.json');
  }

  async runSQL(sqlQuery) {
    if (!this.datapackage) {
      this.datapackage = JSON.parse(await readFile(this.datpackagePath, 'utf-8'));
    }

    const parser = new Parser();
    const ast = parser.parse(sqlQuery);
    const columnNames = ast.columns.map(columnDesc => columnDesc.expr.column);
    const resourcesMap = new Map();

    // console.log(JSON.stringify(ast, null, 2));

    const { conceptTypeHash, entitySetByDomainHash, entityDomainBySetHash } = await getConceptsInfo(this.basePath, this.datapackage);
    const optimFiles = [];

    if (ast.from[0].table === 'datapoints') {
      const idx = JSON.parse(await readFile(path.resolve(this.basePath, 'idx-datapoints.json'), 'utf-8'));
      optimFiles.push(...optimizator(ast.where, idx, conceptTypeHash, entityDomainBySetHash));
    }

    console.log(util.astToSQL(ast));

    for (const conceptDesc of this.datapackage.ddfSchema[ast.from[0].table]) {
      const keys = intersection(conceptDesc.primaryKey, columnNames);
      const values = difference(columnNames, conceptDesc.primaryKey);

      if (keys.length === conceptDesc.primaryKey.length &&
        conceptDesc.primaryKey.length + values.length === columnNames.length &&
        this.notEntities(values, conceptTypeHash)) {
        for (const resource of this.datapackage.resources) {
          if (!isEmpty(optimFiles) && !includes(optimFiles, resource.path)) {
            continue;
          }

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

    const recordFilterFun = getRecordFilterFun(ast);
    return await query(this.basePath, resourcesMap, recordFilterFun, entitySetByDomainHash, entityDomainBySetHash, conceptTypeHash, columnNames);
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
