const path = require('path');
const Session = require('./session');

(async () => {
  console.time('sql');
  try {
    /*const sqlQuery = `
    SELECT geo, time, income_mountains
    FROM datapoints
    WHERE (time=2018 OR time=2019) AND geo='world'
    ORDER BY time`;*/
    const sqlQuery = `SELECT concept, concept_type FROM concepts`;
    const session = new Session(path.resolve('..', 'sg', 'ddf--gapminder--systema_globalis'));
    const result = await session.runSQL(sqlQuery)
    console.timeEnd('sql');
    console.log(result.length);
    console.log(result);
  } catch (e) {
    console.log(e);
  }
})();
