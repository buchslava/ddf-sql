const { intersection, difference } = require('lodash');
const traverse = require('traverse');

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

module.exports = function optimizator(whereClause, idx, conceptTypeHash, entityDomainBySetHash) {
  const logicalOperators = new Map();
  const conjunctionTable = [];
  const disjunctionTable = [];

  traverse(whereClause).forEach(function (obj) {
    if (obj) {
      if (obj.operator === 'AND' || obj.operator === 'OR') {
        logicalOperators.set(this.level, obj.operator)
      }

      const enityConditionDesc = getEntityConditionDescriptor(obj, conceptTypeHash);
      if (enityConditionDesc) {
        const files = [];
        const conditionalValue = enityConditionDesc.attribute ? idx.entityAttributes[enityConditionDesc.attribute] : enityConditionDesc.value;
        const realValues = getValuesSetByEntityConditionDescriptor(enityConditionDesc, conditionalValue, idx, conceptTypeHash, entityDomainBySetHash);

        for (const value of realValues) {
          const entityValuesToDatapointFile = idx.entityValueToDatapointFile[value] || [];
          for (const dpFileId of entityValuesToDatapointFile) {
            files.push(idx.resourcesMap.idToPath[dpFileId.toString()]);
          }
        }

        const op = logicalOperators.get(this.level - 1);
        if (op === 'AND') {
          conjunctionTable.push(...files);
        } else if (op === 'OR') {
          disjunctionTable.push(...files);
        } else {
          throw Error(`Wrong operator ${op}`);
        }
      }
    }
  });

  console.log(intersection(conjunctionTable, disjunctionTable));

  return [];
}
