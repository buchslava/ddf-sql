const fs = require('fs');
const path = require('path');
const { Parser, util } = require('flora-sql-parser');
const { promisify } = require('util');
const { intersection, difference, isEmpty, includes } = require('lodash');
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
    this.diag = {};

    if (!this.datapackage) {
      this.datapackage = JSON.parse(await readFile(this.datpackagePath, 'utf-8'));
    }

    const parser = new Parser();
    const ast = parser.parse(sqlQuery);
    const columnNames = ast.columns.map(columnDesc => columnDesc.expr.column);
    const resourcesMap = new Map();
    const { conceptTypeHash, entitySetByDomainHash, entityDomainBySetHash } = await getConceptsInfo(this.basePath, this.datapackage);
    const allFiles = this.datapackage.resources.map(resource => resource.path);
    const optimFiles = [];

    if (ast.from[0].table === 'datapoints') {
      if (!this.idx) {
        this.idx = JSON.parse(await readFile(path.resolve(this.basePath, 'idx-datapoints.json'), 'utf-8'));
      }

      optimFiles.push(...optimizator(ast, allFiles, this.idx, conceptTypeHash, entityDomainBySetHash));
      this.diag.recommendedFiles = optimFiles;
    }

    this.diag.normalizedSQL = util.astToSQL(ast);

    for (const conceptDesc of this.datapackage.ddfSchema[ast.from[0].table]) {
      const keys = intersection(conceptDesc.primaryKey, columnNames);
      const values = difference(columnNames, conceptDesc.primaryKey);

      if (keys.length === conceptDesc.primaryKey.length &&
        conceptDesc.primaryKey.length + values.length === columnNames.length &&
        includes(values, conceptDesc.value)) {
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

    this.diag.resourcesMap = resourcesMap;
    const { recordFilterFun, source } = getRecordFilterFun(ast);
    this.diag.source = source;

    return await query(this.basePath, resourcesMap, recordFilterFun, entitySetByDomainHash, entityDomainBySetHash, conceptTypeHash, columnNames);
  }
}
