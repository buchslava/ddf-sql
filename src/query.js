const fs = require('fs');
const request = require('request');
// request(`https://raw.githubusercontent.com/open-numbers/ddf--gapminder--systema_globalis/master/${file}`) instead fs.createReadStream
const path = require('path');
const csv = require('csv-parser');
const stripBomStream = require('strip-bom-stream');
const { pick } = require('lodash');

module.exports = async function query(basePath, resourcesMap, recordFilterFun, entitySetByDomainHash, entityDomainBySetHash, conceptTypeHash, columnNamesTemplate) {
  return new Promise((resolve) => {
    let result = [];

    const files = [...resourcesMap.keys()];
    const dataMapping = new Map();
    const readData = (file) => new Promise((resolve, reject) => {
      const relatedKeys = resourcesMap.get(file).keys;
      const relatedValues = resourcesMap.get(file).values;

      fs.createReadStream(path.resolve(basePath, file))
        .pipe(stripBomStream())
        .pipe(csv())
        .on('data', record => {
          let synonyms = new Map();
          const key = relatedKeys.map(recordKey => {
            let ret = record[recordKey];

            if (!ret) {
              for (const synonym of entitySetByDomainHash[recordKey]) {
                if (record[synonym]) {
                  synonyms.set(recordKey, synonym);
                  return record[synonym];
                }
              }
            }

            return ret;
          }).join('@');

          if (!dataMapping.has(key)) {
            const obj = {};

            for (const key1 of relatedKeys) {
              obj[key1] = record[key1];
            }

            // ///////////////////////////
            for (const key1 of relatedKeys) {
              if (conceptTypeHash[key1] === 'entity_set' && !obj[key1]) {
                obj[entityDomainBySetHash[key1]] = record[key1];
              }
            }
            // ///////////////////////////

            dataMapping.set(key, obj);
          }

          const data = dataMapping.get(key);
          for (const [domain, set] of synonyms) {
            data[domain] = record[set];
          }

          for (const value of relatedValues.values()) {
            data[value] = record[value];
            data.valuesCount++;
          }

          if (data.valuesCount >= files.length) {
            delete data.valuesCount;
            result.push(data);
            dataMapping.delete(key);
          } else {
            dataMapping.set(key, data);
          }
        })
        .on('end', resolve)
        .on('error', reject);
    });

    Promise.all(files.map(file => readData(file))).then(() => {
      for (const [, value] of dataMapping.entries()) {
        delete value.valuesCount;
        result.push(value);
      }

      const raw = recordFilterFun ? result.filter(recordFilterFun) : result;

      resolve(raw.map(record => pick(record, columnNamesTemplate)));
    });
  });
}
