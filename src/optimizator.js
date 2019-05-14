const { intersection, difference, concat } = require('lodash');
const uuidv4 = require('uuid/v4');

global._AND = function () {
  return intersection(...arguments);
}

global._OR = function () {
  return concat(...arguments);
}

function getEntityConditionDescriptor(obj, conceptTypeHash) {
  if (!obj) {
    return null;
  }

  const opposite = placement => placement === 'left' ? 'right' : 'left';
  const isColumnRefWithTable = branch => !!obj[branch] && obj[branch].type === 'column_ref' && !!obj[branch].table && !!obj[branch].column;
  const isColumnRefOnly = branch => !!obj[branch] && obj[branch].type === 'column_ref' && !obj[branch].table && !!obj[branch].column;
  const getBooleanValue = branch => !!obj[branch] && obj[branch].type === 'bool' ? obj[branch].value : null;

  let result = null;

  if ((obj.operator === '=' || obj.operator === '<>') && isColumnRefWithTable('left') && getBooleanValue('right')) {
    result = {
      entity: obj.left.table,
      attribute: obj.left.column,
      boolValue: getBooleanValue('right'),
      entityPlacement: 'left',
      isInclude: obj.operator === '='
    };
  }

  if ((obj.operator === '=' || obj.operator === '<>') && isColumnRefWithTable('right') && getBooleanValue('left')) {
    result = {
      entity: obj.right.table,
      attribute: obj.right.column,
      boolValue: getBooleanValue('left'),
      entityPlacement: 'right',
      isInclude: obj.operator === '='
    };
  }

  if (isColumnRefOnly('left') || isColumnRefOnly('right')) {
    const entityPlacement = isColumnRefOnly('left') ? 'left' : 'right';
    const entity = obj[entityPlacement].column;
    const attribute = null;
    const isInclude = obj.operator === '=' || obj.operator === 'IN';
    const value = obj[opposite(entityPlacement)].type === 'expr_list' ? obj[opposite(entityPlacement)].value.map(r => r.value) : [obj[opposite(entityPlacement)].value];

    result = { entity, attribute, value, entityPlacement, isInclude };
  }

  if (!(result && result.entity && (conceptTypeHash[result.entity] === 'entity_set' || conceptTypeHash[result.entity] === 'entity_domain'))) {
    result = null;
  }

  return result;
}

function getValuesSetByEntityConditionDescriptor(enityConditionDesc, values, idx, conceptTypeHash, entityDomainBySetHash) {
  if (enityConditionDesc.isInclude) {
    return values;
  }

  const result = [];
  const keys = Object.keys(idx.entityDomains);
  const template = conceptTypeHash[enityConditionDesc.entity] === 'entity_domain' ? `${enityConditionDesc.entity}@` : `${entityDomainBySetHash[enityConditionDesc.entity]}@{enityConditionDesc.entity}`;

  for (const key of keys) {
    if (key.indexOf(template) === 0) {
      result.push(...difference(idx.entityDomains[key], values))
    }
  }

  return result;
}

module.exports = function optimizator(ast, allFiles, idx, conceptTypeHash, entityDomainBySetHash) {
  function getFilesConditionsExpression(whereClause) {
    let result = '';

    const commaBoundary = uuidv4();
    const openParenthesesBoundary = uuidv4();
    const closeParenthesesBoundary = uuidv4();

    function processBranch(branch) {
      let isOp = false;

      if (branch && typeof branch === 'object' && (branch.operator === 'AND' || branch.operator === 'OR')) {
        result += `_${branch.operator}${openParenthesesBoundary}`;
        isOp = true;
      } else if (branch.right && branch.left && (branch.left.column || branch.left.table)) {
        const enityConditionDesc = getEntityConditionDescriptor(branch, conceptTypeHash);
        if (enityConditionDesc) {
          const files = [];
          const conditionalValue = enityConditionDesc.attribute ? idx.entityAttributes[enityConditionDesc.attribute] : enityConditionDesc.value;
          const realValues = getValuesSetByEntityConditionDescriptor(enityConditionDesc, conditionalValue, idx, conceptTypeHash, entityDomainBySetHash);

          for (const value of realValues) {
            const entityValuesToDatapointFile = idx.entityValuesToDatapointFile[value] || [];

            for (const dpFileId of entityValuesToDatapointFile) {
              files.push(idx.resourcesMap.idToPath[dpFileId.toString()]);
            }
          }

          result += `${commaBoundary}${JSON.stringify(files)}${commaBoundary}`;

          if (enityConditionDesc.attribute) {
            branch.operator = 'IN';
            branch.left.column = branch.left.table;
            branch.left.table = null;
            branch.right = {
              type: 'expr_list',
              value: conditionalValue.map(v => ({ type: 'string', value: v }))
            };
          }
        } else {
          result += `${commaBoundary}${JSON.stringify(allFiles)}${commaBoundary}`;
        }
      }

      if (branch && typeof branch === 'object') {
        if (branch.left) {
          processBranch(branch.left);
        }
        if (branch.right) {
          processBranch(branch.right);
        }
      }

      if (isOp) {
        result += closeParenthesesBoundary;
      }
    }

    processBranch(whereClause);

    const fixCommaRegexp = new RegExp(`(${commaBoundary})+`, 'g');
    const fixOpenParenthesesRegexp = new RegExp(`${openParenthesesBoundary}${commaBoundary}`, 'g');
    const fixCloseParenthesesRegexp = new RegExp(`${commaBoundary}${closeParenthesesBoundary}`, 'g');
    const commaRegexp = new RegExp(`${commaBoundary}`, 'g');
    const openParenthesesRegexp = new RegExp(`${openParenthesesBoundary}`, 'g');
    const closeParenthesesRegexp = new RegExp(`${closeParenthesesBoundary}`, 'g');

    return result
      .replace(fixCommaRegexp, commaBoundary)
      .replace(fixOpenParenthesesRegexp, openParenthesesBoundary)
      .replace(fixCloseParenthesesRegexp, closeParenthesesBoundary)
      .replace(commaRegexp, ',')
      .replace(openParenthesesRegexp, '(')
      .replace(closeParenthesesRegexp, ')');
  }

  const result = getFilesConditionsExpression(ast.where);
  const resultFun = new Function(`return ${result};`);

  return resultFun();
}
