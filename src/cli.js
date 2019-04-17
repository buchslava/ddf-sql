const Table = require('easy-table')
const Session = require('./session');

/*
SELECT concept, concept_type FROM concepts

https://github.com/open-numbers/ddf--unpop--wpp_population/blob/master/

SELECT geo,year,gender,age,population FROM datapoints WHERE
(geo.un_state=true OR geo.is--global=true OR geo.is--world_4region=true) AND year=2018
AND gender IN ('female') AND age NOT IN ('80plus','100plus') ORDER BY year
*/
const readline = require('readline');
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
  prompt: 'ddf-sql>'
});

let buffer = [];
let session;

rl.prompt();

function query(printResult = true) {
  return async () => {
    if (!session) {
      console.log('session is undefined!');
      buffer = [];
      rl.prompt();
      return;
    }

    const metricLabel = 'query execution time';
    const sql = buffer.join(' ');
    try {
      console.time(metricLabel);
      const result = await session.runSQL(sql);
      console.timeEnd(metricLabel);
      if (printResult) {
        console.log(Table.print(result));
      }
      console.log(`${result.length} were selected...`);
    } catch (e) {
      console.log(e);
    } finally {
      buffer = [];
      rl.prompt();
    }
  };
}

const commands = {
  use: () => {
    const dsPath = buffer.join(' ');
    session = new Session(dsPath);
    buffer = [];
    console.log(`session for "${session.basePath}"`);
    rl.prompt();
  },
  session: () => {
    console.log(`session for "${session.basePath}"`);
    buffer = [];
    rl.prompt();
  },
  sql: query(),
  total: query(false),
  diag: () => {
    console.log(JSON.stringify(session.diag, null, 2));
  },
  q: () => {
    rl.close();
  }
};

rl.on('line', input => {
  if (input in commands) {
    commands[input]();
  } else {
    buffer.push(input);
  }
});
