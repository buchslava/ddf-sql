const { isEmpty } = require('lodash');
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

function getAstBranchesToUpdate(whereClause) {
  const data = [];
  let modifySelectionFilesList = true;

  // console.log(JSON.stringify(whereClause, null, 2));

  traverse(whereClause).forEach(function (obj) {
    if (obj) {
      const enityConditionDesc = getEntityConditionDescriptor(obj);

      if (enityConditionDesc) {
        if (!enityConditionDesc.typeIs) {
          modifySelectionFilesList = false;
        }

        data.push({
          branchToUpdate: this,
          enityConditionDesc
        });
      }
    }
  });

  return { data, modifySelectionFilesList };
}

module.exports = function optimizator(whereClause, datapackage) {
  const foo = getAstBranchesToUpdate(whereClause);

  for (const desc of foo.data) {
    // change criteria !
    if (foo.modifySelectionFilesList) {
    // if (false) {
      desc.branchToUpdate.update({
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
    } else {
      console.log(JSON.stringify(desc.branchToUpdate.node, null, 2));

      const valueToUpdate = Object.assign({}, desc.branchToUpdate.node);

      console.log('---------', valueToUpdate.left.table, valueToUpdate.left.column);

      valueToUpdate.operator = 'IN';
      valueToUpdate.left.column = valueToUpdate.left.table;
      valueToUpdate.left.table = null;
      valueToUpdate.right = {
        type: 'expr_list',
        value: [{ type: 'string', value: 'afg' }]
      };
      desc.branchToUpdate.update(valueToUpdate);
    }
  }

  return {
    enityConditionDescs: foo.data.map(r => r.enityConditionDesc)
    // enityConditionDescs: []
  };
}
