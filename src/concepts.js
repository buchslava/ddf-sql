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
      const conceptTypeHash = {};
      const entityDomainBySetHash = {};
      const entitySetByDomainHash = {};

      for (const record of result) {
        conceptTypeHash[record.concept] = record.concept_type;

        if (record.concept_type === 'entity_set') {
          if (!entitySetByDomainHash[record.domain]) {
            entitySetByDomainHash[record.domain] = [];
          }

          entitySetByDomainHash[record.domain].push(record.concept);
          entityDomainBySetHash[record.concept] = record.domain
        }
      }

      resolve({ conceptTypeHash, entitySetByDomainHash, entityDomainBySetHash });
    });
  });
}
