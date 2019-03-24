function replaceBetween(where, what, start, end) {
  return where.substring(0, start) + what + where.substring(end);
}

function regexIndexOf(str, regexArr, startpos = 0) {
  for (const regex of regexArr) {
    const indexOf = str.substring(startpos || 0).search(regex);

    if (indexOf >= 0) {
      return indexOf + startpos;
    }
  }

  return -1;
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

function getWhereCaluseString(query) {
  const whereStartMarker = 'WHERE ';
  const whereEndMarkers = [/GROUP\s+BY/, /ORDER\s+BY/, /LIMIT/];
  const sPos = query.indexOf(whereStartMarker);

  let result = null;

  if (sPos >= 0) {
    let ePos = regexIndexOf(query, whereEndMarkers, sPos);

    if (ePos < 0) {
      ePos = query.length;
    }

    result = query.substring(sPos + whereStartMarker.length, ePos);
  }

  return result;
}

function processInClauses(whereClauseStr) {
  function describeAll() {
    const result = [];
    const pattern = /(\w+\s+IN\s+\(.*?\))/gi;
    let matches;
  
    while (matches = pattern.exec(whereClauseStr)) {
      result.push({
        sqlClause: matches[1],
        start: matches.index,
        end: pattern.lastIndex
      });
    }
    return result;
  }

  function describeOne(cd) {
    const match = /(\w+)\s+IN\s+\((.*?)\)/.exec(cd.sqlClause);
    const valuesList = match[2].split(',');
    const column = match[1];
    const subClauses = [];

    for (const value of valuesList) {
      subClauses.push(`${column}=${value}`);
    }

    cd.preudoJsClause = `(${subClauses.join(' || ')})`;

    return cd;
  }

  function changeSqlToJs(inClausesDesc) {
    const cds = inClausesDesc.reverse();

    for (const cd of cds) {
      whereClauseStr = replaceBetween(whereClauseStr, cd.preudoJsClause, cd.start, cd.end);
    }

    return whereClauseStr;
  }

  return changeSqlToJs(describeAll().map(inClauseDesc => describeOne(inClauseDesc)));
}

module.exports = function getRecordFilterFun(query, ast) {
  let whereClauseStr = getWhereCaluseString(query);

  if (whereClauseStr === null) {
    return null;
  }

  whereClauseStr = processInClauses(whereClauseStr);

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
}