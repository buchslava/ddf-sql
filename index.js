const fs = require('fs');
const path = require('path');
const { Parser } = require('flora-sql-parser');
const { promisify } = require('util');
const { intersection, difference, isEmpty, includes, filter, map } = require('lodash');
const query = require('./query');
const readFile = promisify(fs.readFile);
const basePath = path.resolve('..', 'sg', 'ddf--gapminder--systema_globalis');
const datpackagePath = path.resolve(basePath, 'datapackage.json');

function regexIndexOf(str, regexArr, startpos = 0) {
  for (const regex of regexArr) {
    const indexOf = str.substring(startpos || 0).search(regex);

    if (indexOf >= 0) {
      return indexOf + startpos;
    }
  }

  return -1;
}

function getWhereCaluseString(query) {
  const whereStartMarker = 'WHERE ';
  const whereEndMarkers = [/GROUP\s+BY/, /ORDER\s+BY/, /LIMIT/];
  const sPos = query.indexOf(whereStartMarker);

  let result = '';

  if (sPos >= 0) {
    let ePos = regexIndexOf(query, whereEndMarkers, sPos);

    if (ePos < 0) {
      ePos = query.length;
    }

    result = query.substring(sPos + whereStartMarker.length, ePos);
  }

  return result;
}

function getColumnsFromWhereClause(whereClause) {
  function iterate(obj) {
    for (const property in obj) {
      if (obj.hasOwnProperty(property)) {
        if (typeof obj[property] === 'object') {
          iterate(obj[property]);
        } else {
          if (property === 'column') {
            columns.push(obj[property]);
          }
        }
      }
    }
  }

  const columns = [];

  iterate(whereClause);
  return columns;
}

function getRecordFilterFun(query, ast) {
  let whereClauseStr = getWhereCaluseString(query);

  const columnsFromWhereClause = getColumnsFromWhereClause(ast.where);
  const processed = new Set();

  for (const column of columnsFromWhereClause) {
    if (!processed.has(column)) {
      whereClauseStr = whereClauseStr.replace(new RegExp(column, 'gmi'), `record['${column}']`);
    }
    
    processed.add(column);
  }

  whereClauseStr = whereClauseStr.replace(new RegExp('\\s+AND\\s+', 'gmi'), ' && ');
  whereClauseStr = whereClauseStr.replace(new RegExp('\\s+OR\\s+', 'gmi'), ' || ');
  whereClauseStr = whereClauseStr.replace(new RegExp('\\s?=\\s?', 'gmi'), '==');
  whereClauseStr = `return ${whereClauseStr};`;

  return new Function('record', whereClauseStr);

  /*console.log(filterFun({time: 2018, geo: 'world'}));
  console.log(filterFun({time: 2019, geo: 'world'}));
  console.log(filterFun({}));
  console.log(filterFun({time: 2010, geo: 'world'}));
  console.log(filterFun({time: 2018, geo: 'foo'}));*/
}

(async () => {
  try {
    const datapackage = JSON.parse(await readFile(datpackagePath, 'utf-8'));

    /*const sql = `
    SELECT geo, time, income_mountains
    FROM datapoints
    WHERE (time=2018 OR time=2019) AND geo='world'
    ORDER BY time`;*/
    const sql = `SELECT concept, concept_type FROM concepts`;

    const parser = new Parser();
    const ast = parser.parse(sql);

    const columnNames = ast.columns.map(columnDesc => columnDesc.expr.column);

    const resourcesMap = new Map();
    for (const conceptDesc of datapackage.ddfSchema.concepts) {
      const keys = intersection(conceptDesc.primaryKey, columnNames);
      const values = difference(columnNames, conceptDesc.primaryKey);

      if (!isEmpty(keys) && includes(values, conceptDesc.value)) {
        for (const resource of datapackage.resources) {
          if (includes(conceptDesc.resources, resource.name)) {
            if (!resourcesMap.has(resource.path)) {
              resourcesMap.set(resource.path, {keys, values: new Set()});
            }

            const resourceDesc = resourcesMap.get(resource.path);
            resourceDesc.values.add(conceptDesc.value);
  
            resourcesMap.set(resource.path, resourceDesc);  
          }
        }
      }
    }

    console.time('foo');
    const r = await query(basePath, resourcesMap);
    console.timeEnd('foo');
    console.log(r.length);

    // getRecordFilterFun(query, ast);
    // console.log(JSON.stringify(ast.where, null, 2));
    // console.log(columnNames);
  } catch (e) {
    console.log(e);
  }
})();
