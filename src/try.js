const Session = require('./session');

console.time('sql');
const session = new Session('../pop');

/*const sql = `
SELECT geo,year,gender,age,population FROM datapoints WHERE
(geo.un_state=true OR geo.is--global=true OR geo.is--world_4region=true) AND year=2018
AND gender IN ('female') AND age NOT IN ('80plus','100plus') ORDER BY year`;*/

/*const sql = `
SELECT geo,year,gender,age,population FROM datapoints WHERE
(geo.un_state=true OR geo.is__global=true OR geo.is__world_4region=true) AND year=2018
AND gender IN ('female') ORDER BY year`;*/

const sql = `
SELECT geo,year,gender,age,population FROM datapoints WHERE
(geo.is__global=true OR geo.is__world_4region=true) AND year=2018
AND gender IN ('female') ORDER BY year`;

(async ()=> {
  /*const result2 = await session.runSQL(`SELECT geo FROM entities WHERE un_state='TRUE'`);
  console.log(result2);*/

  const result = await session.runSQL(sql);
  console.timeEnd('sql');
  console.log(result);
})();
