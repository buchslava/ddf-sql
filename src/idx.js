const fs = require('fs');
const { isArray, includes, intersection } = require('lodash');
const { resolve } = require('path');
const { promisify } = require('util');
const csv = require('csv-parser');
const stripBomStream = require('strip-bom-stream');
const readFile = promisify(fs.readFile);
const writeFile = promisify(fs.writeFile);
const getConceptsInfo = require('./concepts');

async function readCsv(file) {
  const result = [];
  const readData = (file) => new Promise((resolve, reject) => {
    fs.createReadStream(file)
      .pipe(stripBomStream())
      .pipe(csv())
      .on('data', record => result.push(record))
      .on('end', resolve)
      .on('error', reject);
  });

  await readData(file);
  return result;
}

function getRecourcesMaps(datapackage) {
  const idToPath = {};
  const pathToId = {};
  let num = 1;

  for (const resource of datapackage.resources) {
    idToPath[num] = resource.path;
    pathToId[resource.path] = num;
    ++num;
  }

  return { idToPath, pathToId };
}

(async () => {
  const basePath = resolve('..', 'pop');
  const datpackagePath = resolve(basePath, 'datapackage.json');
  const datapackage = JSON.parse(await readFile(datpackagePath, 'utf-8'));
  const { conceptTypeHash, entityDomainBySetHash } = await getConceptsInfo(basePath, datapackage);
  const resourcesMaps = getRecourcesMaps(datapackage);
  const entityDomains = {};
  const entityValuesToDatapointFile = {};
  const entityAttributes = {};

  for (const resource of datapackage.resources) {
    const concept = resource.schema.primaryKey;

    if (!isArray(concept) && concept !== 'concept') {
      if (conceptTypeHash[concept] === 'entity_set' || conceptTypeHash[concept] === 'entity_domain') {
        const domain = conceptTypeHash[concept] === 'entity_set' ? entityDomainBySetHash[concept] : concept;
        const key = conceptTypeHash[concept] === 'entity_set' ? `${domain}@${concept}` : `${domain}@*`;
        const content = await readCsv(resolve(basePath, resource.path));
        const entitiesIds = [];
        const isFields = [];

        for (const record of content) {
          const entityId = record[concept] || record[domain];

          for (const field of Object.keys(record)) {
            if ((field.indexOf('is--') === 0 || conceptTypeHash[field] === 'boolean') && record[field].toLowerCase() === 'true') {
              const normField = field.replace('--', '__');

              if (!entityAttributes[normField]) {
                entityAttributes[normField] = [];
              }

              entityAttributes[normField].push(entityId);

              if (field.indexOf('is__') === 0) {
                isFields.push(field.replace('is__', ''));
              }
            }
          }

          entitiesIds.push(entityId);
        }

        for (const dpResource of datapackage.resources) {
          const dpPrimaryKey = dpResource.schema.primaryKey;
          const areIsFieldsExisting = intersection(isFields, dpPrimaryKey).length > 0;

          if (isArray(dpPrimaryKey) && (includes(dpPrimaryKey, domain) || includes(dpPrimaryKey, concept) || areIsFieldsExisting)) {
            const dpContent = await readCsv(resolve(basePath, dpResource.path));

            for (const record of dpContent) {
              for (const isField of isFields) {
                if (record[isField]) {
                  if (!entityValuesToDatapointFile[record[isField]]) {
                    entityValuesToDatapointFile[record[isField]] = new Set();
                  }

                  entityValuesToDatapointFile[record[isField]].add(resourcesMaps.pathToId[dpResource.path]);
                }
              }

              if (record[concept || domain]) {
                if (!entityValuesToDatapointFile[record[concept || domain]]) {
                  entityValuesToDatapointFile[record[concept || domain]] = new Set();
                }

                entityValuesToDatapointFile[record[concept || domain]].add(resourcesMaps.pathToId[dpResource.path]);
              }
            }
          }
        }

        entityDomains[key] = entitiesIds;
      }
    }
  }

  for (const key of Object.keys(entityValuesToDatapointFile)) {
    entityValuesToDatapointFile[key] = [...entityValuesToDatapointFile[key]];
  }

  const resourcesMap = { idToPath: resourcesMaps.idToPath, pathToId: resourcesMaps.pathToId };

  await writeFile('idx-datapoints.json', JSON.stringify({ entityDomains, entityAttributes, entityValuesToDatapointFile, resourcesMap }, null, 2));
})();
