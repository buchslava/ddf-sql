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
      if (conceptTypeHash[concept] === 'entity_set' || conceptTypeHash[concept] === 'entity_domain') {
        const domain = conceptTypeHash[concept] === 'entity_set' ? entityDomainBySetHash[resource.schema.primaryKey] : resource.schema.primaryKey;
        const key = conceptTypeHash[concept] === 'entity_set' ? `${domain}@${concept}` : `${domain}@*`;
        const content = await readCsv(resolve(basePath, resource.path));
        const entitiesIds = [];
        const isFields = [];

        for (const record of content) {
          const entityId = record[concept] || record[domain];

          for (const field of Object.keys(record)) {
            if ((field.indexOf('is--') === 0 || conceptTypeHash[field] === 'boolean') && record[field].toLowerCase() === 'true') {
              if (!entityAttributesMap.has(field)) {
                entityAttributesMap.set(field.replace('--', '__'), []);
              }

              entityAttributesMap.get(field.replace('--', '__')).push(entityId);

              if (field.indexOf('is--') === 0) {
                isFields.push(field.replace('is--', ''));
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
                  if (!entityValueToDapapointFileMap.has(record[isField])) {
                    entityValueToDapapointFileMap.set(record[isField], new Set());
                  }

                  entityValueToDapapointFileMap.get(record[isField]).add(resourcesMaps.pathToId.get(dpResource.path));
                }
              }

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
  const entityValueToDatapointFile = {};
  for (const [k, v] of entityValueToDapapointFileMap) {
    entityValueToDatapointFile[k] = [...v];
  }
  const entityAttributes = mapToObj(entityAttributesMap);
  const idToPath = mapToObj(resourcesMaps.idToPath);
  const pathToId = mapToObj(resourcesMaps.pathToId);
  const resourcesMap = { idToPath, pathToId };

  await writeFile('idx-datapoints.json', JSON.stringify({ entityDomains, entityAttributes, entityValueToDatapointFile, resourcesMap }, null, 2));
})();
