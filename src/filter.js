const { Parser, util } = require('flora-sql-parser');

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
  const whereStartMarker = /WHERE\s+/;
  const whereEndMarkers = [/GROUP\s+BY/, /ORDER\s+BY/, /LIMIT/];
  const sPos = regexIndexOf(query, [whereStartMarker]);

  let result = null;

  if (sPos >= 0) {
    let ePos = regexIndexOf(query, whereEndMarkers, sPos);

    if (ePos < 0) {
      ePos = query.length;
    }

    result = query.substring(sPos + 5, ePos);
  }

  return result;
}

function prepareListClausesProcessing(generalPattern, clausePattern, eqOp, logicalConcatOp) {
  return function (whereClauseStr) {
    function describeAll() {
      const result = [];
      let matches;

      while (matches = generalPattern.exec(whereClauseStr)) {
        result.push({
          sqlClause: matches[1],
          start: matches.index,
          end: generalPattern.lastIndex
        });
      }
      return result;
    }

    function describeOne(cd) {
      const match = clausePattern.exec(cd.sqlClause);
      const valuesList = match[2].split(',');
      const column = match[1];
      const subClauses = [];

      for (const value of valuesList) {
        subClauses.push(`${column}${eqOp}${value}`);
      }

      cd.preudoJsClause = `(${subClauses.join(' ' + logicalConcatOp + ' ')})`;

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
}

function processInClauses(whereClauseStr) {
  return prepareListClausesProcessing(
    /(\w+\s+IN\s+\(.*?\))/gi,
    /(\w+)\s+IN\s+\((.*?)\)/i,
    '=', '||')(whereClauseStr);
}

function processNotInClauses(whereClauseStr) {
  return prepareListClausesProcessing(
    /(\w+\s+NOT\s+IN\s+\(.*?\))/gi,
    /(\w+)\s+NOT\s+IN\s+\((.*?)\)/i,
    '<>', '&&')(whereClauseStr);
}

module.exports = function getRecordFilterFun(ast) {
  const query = util.astToSQL(ast);
  let whereClauseStr = getWhereCaluseString(query);

  if (whereClauseStr === null) {
    return null;
  }

  const columnsFromWhereClause = getColumnsFromWhereClause(ast.where);

  for (const column of columnsFromWhereClause) {
    whereClauseStr = whereClauseStr.replace(new RegExp(`"${column}"`, 'gmi'), column);
  }

  whereClauseStr = processNotInClauses(whereClauseStr);
  whereClauseStr = processInClauses(whereClauseStr);

  const processed = new Set();

  for (const column of columnsFromWhereClause) {
    if (!processed.has(column)) {
      whereClauseStr = whereClauseStr.replace(new RegExp(`(?<start>[^'])(?<cName>${column})(?<end>[^'])`, 'gmi'), `$<start>record['$<cName>']$<end>`);
    }

    processed.add(column);
  }

  whereClauseStr = whereClauseStr.replace(new RegExp('\\s+AND\\s+', 'gmi'), ' && ');
  whereClauseStr = whereClauseStr.replace(new RegExp('\\s+OR\\s+', 'gmi'), ' || ');
  whereClauseStr = whereClauseStr.replace(new RegExp('\\s?=\\s?', 'gmi'), '==');
  whereClauseStr = whereClauseStr.replace(new RegExp('\\s?<>\\s?', 'gmi'), '!=');
  whereClauseStr = `return ${whereClauseStr};`;

  return new Function('record', whereClauseStr);
}
