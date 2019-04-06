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

async function loadEntities() {
  return new Promise(resolve => {
    resolve();
  });
}

module.exports = async function foo(whereClause, datapackage) {
  // console.log(JSON.stringify(whereClause, null, 2));
  const enityConditionDescs = [];

  function iterate(obj) {
    for (const property in obj) {
      if (obj.hasOwnProperty(property)) {
        if (typeof obj[property] === 'object') {
          const enityConditionDesc = getEntityConditionDescriptor(obj[property]);

          if (enityConditionDesc) {
            enityConditionDescs.push(enityConditionDesc);
            continue;
          }

          iterate(obj[property]);
        }
      }
    }
  }

  return new Promise(async (resolve) => {
    await loadEntities();
    iterate(whereClause);
    console.log(enityConditionDescs);
  
    resolve();
  });
}
