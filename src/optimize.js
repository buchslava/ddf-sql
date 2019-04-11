const traverse = require('traverse');

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

module.exports = function foo(whereClause, datapackage) {
  const enityConditionDescs = [];

  traverse(whereClause).forEach(function (obj) {
    let shouldBeExcluded = false;

    if (typeof obj === 'object') {
      const enityConditionDesc = getEntityConditionDescriptor(obj);

      if (enityConditionDesc) {
        enityConditionDescs.push(enityConditionDesc);
        shouldBeExcluded = true;
      }
    }

    if (shouldBeExcluded) {
      this.update({
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
  });

  return {
    enityConditionDescs
  };
}
