const fs = require('fs');
const { isArray, includes } = require('lodash');
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

function mapToObj(map) {
  return Array.from(map).reduce((obj, [key, value]) => (
    Object.assign(obj, { [key]: value })
  ), {});
}

function getRecourcesMaps(datapackage) {
  const idToPath = new Map();
  const pathToId = new Map();
  let num = 1;

  for (const resource of datapackage.resources) {
    idToPath.set(num, resource.path);
    pathToId.set(resource.path, num);
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
  const entityDomainsMap = new Map();
  const entityValueToDapapointFileMap = new Map();
  const entityAttributesMap = new Map();

  for (const resource of datapackage.resources) {
    const concept = resource.schema.primaryKey;

    if (!isArray(concept) && concept !== 'concept') {
      if (conceptTypeHash[concept] === 'entity_set') {
        const domain = entityDomainBySetHash[resource.schema.primaryKey];
        const key = `${domain}@${concept}`;
        const content = await readCsv(resolve(basePath, resource.path));
        // const entitiesValues = content.map(r => r[concept] || r[domain]);
        const entitiesIds = [];

        for (const record of content) {
          const entityId = record[concept] || record[domain];

          for (const field of Object.keys(record)) {
            if ((field.indexOf('is--') === 0 || conceptTypeHash[field] === 'boolean') && record[field].toLowerCase() === 'true') {
              if (!entityAttributesMap.has(field)) {
                entityAttributesMap.set(field, []);
              }

              entityAttributesMap.get(field).push(entityId);
            }
          }

          entitiesIds.push(entityId);
        }

        for (const dpResource of datapackage.resources) {
          const dpPrimaryKey = dpResource.schema.primaryKey;

          if (isArray(dpPrimaryKey) && (includes(dpPrimaryKey, domain) || includes(dpPrimaryKey, concept))) {
            const dpContent = await readCsv(resolve(basePath, dpResource.path));

            for (const record of dpContent) {
              if (!entityValueToDapapointFileMap.has(record[concept || domain])) {
                entityValueToDapapointFileMap.set(record[concept || domain], new Set());
              }

              entityValueToDapapointFileMap.get(record[concept || domain]).add(resourcesMaps.pathToId.get(dpResource.path));
            }
          }
        }

        entityDomainsMap.set(key, entitiesIds);
      }
    }
  }

  const entityDomains = mapToObj(entityDomainsMap);
  const entityValueToDapapointFile = {};
  for (const [k, v] of entityValueToDapapointFileMap) {
    entityValueToDapapointFile[k] = [...v];
  }
  const resources = mapToObj(resourcesMaps);
  const entityAttributes = mapToObj(entityAttributesMap);

  await writeFile('idx-datapoints.json', JSON.stringify({ entityDomains, entityAttributes, entityValueToDapapointFile, resources }, null, 2));
})();
