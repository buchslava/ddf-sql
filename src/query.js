const fs = require('fs');
const request = require('request');
const path = require('path');
const csv = require('csv-parser');
const stripBomStream = require('strip-bom-stream');

module.exports = async function query(basePath, resourcesMap, recordFilterFun) {
  return new Promise((resolve) => {
    let result = [];

    const files = [...resourcesMap.keys()];
    const dataMapping = new Map();
    const readData = (file) => new Promise((resolve, reject) => {
      const relatedKeys = resourcesMap.get(file).keys;
      const relatedValues = resourcesMap.get(file).values;

      // request(`https://raw.githubusercontent.com/open-numbers/ddf--gapminder--systema_globalis/master/${file}`)

      fs.createReadStream(path.resolve(basePath, file))
        .pipe(stripBomStream())
        .pipe(csv())
        .on('data', record => {
          const key = relatedKeys.map(recordKey => record[recordKey]).join('@');

          if (!dataMapping.has(key)) {
            const obj = {};

            for (const key1 of relatedKeys) {
              obj[key1] = record[key1];
            }
            dataMapping.set(key, obj);
          }

          const data = dataMapping.get(key);

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

      if (recordFilterFun) {
        resolve(result.filter(recordFilterFun));
      } else {
        resolve(result);
      }
    });
  });
}
