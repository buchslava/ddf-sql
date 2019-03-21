const _ = require('lodash');
const fs = require('fs');
const path = require('path');
const etl = require('etl');
const basePath = path.resolve('..', 'sg', 'ddf--gapminder--systema_globalis');
const filesDescriptors = [
  {
    value: 'yearly_co2_emissions_1000_tonnes',
    name: path.resolve(basePath, 'ddf--datapoints--yearly_co2_emissions_1000_tonnes--by--geo--time.csv')
  },
  {
    value: 'urban_poverty_percent_urban_people_below_national_urban',
    name: path.resolve(basePath, 'ddf--datapoints--urban_poverty_percent_urban_people_below_national_urban--by--geo--time.csv')
  },
  {
    value: 'children_per_woman_total_fertility_with_projections',
    name: path.resolve(basePath, 'ddf--datapoints--children_per_woman_total_fertility_with_projections--by--geo--time.csv')
  }
];

function readAll() {
  let result = [];

  const dataMapping = new Map();
  const readData = (fileDesc) => new Promise((resolve, reject) => {
    fs.createReadStream(fileDesc.name)
      .pipe(etl.csv())
      .pipe(etl.map(record => {
        const key = `${record.geo}@${record.time}`;

        if (!dataMapping.has(key)) {
          dataMapping.set(key, {
            geo: record.geo,
            time: record.time,
            valuesCount: 0
          });
        }

        const data = dataMapping.get(key);
        data[fileDesc.value] = record[fileDesc.value];
        data.valuesCount++;

        if (data.valuesCount >= filesDescriptors.length) {
          delete data.valuesCount;
          result.push(data);
          dataMapping.delete(key);
        } else {
          dataMapping.set(key, data);
        }
      }))
      .promise()
      .then(resolve, reject);
  });

  console.time('q');

  Promise.all(filesDescriptors.map(fileDesc => readData(fileDesc))).then(() => {
    for (const [, value] of dataMapping.entries()) {
      delete value.valuesCount;
      result.push(value);
    }

    result = _.sortBy(result, [o => o.time]);
    console.log(result.length);
    console.timeEnd('q');
  });
}

readAll();
