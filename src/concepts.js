const fs = require('fs');
const path = require('path');
const etl = require('etl');

module.exports = async function getConceptsInfo(basePath, datapackage) {
  const files = datapackage.resources.filter(r => r.schema.primaryKey === 'concept').map(r => r.path);

  return new Promise((resolve) => {
    const result = [];
    const readData = (file) => new Promise((resolve, reject) => {
      fs.createReadStream(path.resolve(basePath, file))
        .pipe(etl.csv())
        .pipe(etl.map(record => result.push(record)))
        .promise()
        .then(resolve, reject);
    });

    Promise.all(files.map(file => readData(file))).then(() => {
      const conceptTypeHash = result.reduce((hash, record) => {
        hash[record.concept] = record.concept_type;
        return hash;
      }, {});
      resolve({conceptTypeHash});
    });
  });
}
