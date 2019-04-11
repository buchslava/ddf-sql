const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const stripBomStream = require('strip-bom-stream');

module.exports = async function getConceptsInfo(basePath, datapackage) {
  const files = datapackage.resources.filter(r => r.schema.primaryKey === 'concept').map(r => r.path);

  return new Promise((resolve) => {
    const result = [];
    const readData = (file) => new Promise((resolve, reject) => {
      fs.createReadStream(path.resolve(basePath, file))
        .pipe(stripBomStream())
        .pipe(csv())
        .on('data', record => result.push(record))
        .on('end', resolve)
        .on('error', reject);
    });

    Promise.all(files.map(file => readData(file))).then(() => {
      const conceptTypeHash = result.reduce((hash, record) => {
        hash[record.concept] = record.concept_type;
        return hash;
      }, {});

      resolve({ conceptTypeHash });
    });
  });
}
