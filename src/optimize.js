const traverse = require('traverse');

function getColumnRef(obj) {
  if (obj && obj.left && obj.left.type === 'column_ref') {
    return obj.left.table || obj.left.column;
  }

  if (obj && obj.right && obj.right.type === 'column_ref') {
    return obj.right.table || obj.right.column;
  }

  return null;
}

function getEntityConditionDescriptor(obj) {
  const isLeftProper = () => !!obj.left && obj.left.type === 'column_ref' && !!obj.left.table && !!obj.left.column;
  const isRightProper = () => !!obj.right && obj.right.type === 'bool' && obj.right.value === true;

  if (obj && obj.operator === '=' && isLeftProper() && isRightProper()) {
    return {
      entity: obj.left.table,
      property: obj.left.column,
      typeIs: obj.left.column.indexOf('is__') >= 0
    };
  }

  return null;
}

function getAstBranchesToUpdate(whereClause) {
  const levelsOfConcepts = {};
  const branchesToUpdate = [];
  const enityConditionDescs = [];
  let levelsOfDisjunction = [];

  traverse(whereClause).forEach(function (obj) {
    if (obj) {
      const enityConditionDesc = getEntityConditionDescriptor(obj);
      const columnRef = getColumnRef(obj);
      let record = null;

      if (columnRef) {
        record = {concept: columnRef, level: this.level};
      }

      if (enityConditionDesc) {
        record = {concept: enityConditionDesc.entity, level: this.level};
        branchesToUpdate.push(this);
        enityConditionDescs.push(enityConditionDesc);
      }

      if (record) {
        if (!levelsOfConcepts[record.concept]) {
          levelsOfConcepts[record.concept] = new Set();
        }

        levelsOfConcepts[record.concept].add(record.level);
      }

      if (obj.type === 'binary_expr' && obj.operator == 'OR') {
        levelsOfDisjunction.push(this.level);
      }
    }
  });

  levelsOfDisjunction = levelsOfDisjunction.sort();

  let isAllowed = true;

  for (const concept of Object.keys(levelsOfConcepts)) {
    if (levelsOfConcepts[concept].size > 1 && levelsOfDisjunction.length > 0) {
      isAllowed = false;
      break;
    }
  }

  return isAllowed ? {enityConditionDescs, branchesToUpdate} : {};
}

module.exports = function optimizator(whereClause, datapackage) {
  const { enityConditionDescs, branchesToUpdate } = getAstBranchesToUpdate(whereClause);

  for (const branch of branchesToUpdate) {
    branch.update({
      type: 'binary_expr',
      operator: '=',
      left: {
        type: 'number',
        value: 1
      },
      right: {
        type: 'number',
        value: 1
      }
    });
  }

  return {
    enityConditionDescs
  };
}
