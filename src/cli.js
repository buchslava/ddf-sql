const Table = require('easy-table')
const chalk = require('chalk');
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
      console.log(chalk.red('session is undefined!'));
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
        console.log(chalk.yellow(Table.print(result)));
      }
      console.log(chalk.blue(`${result.length} were selected...`));
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
    console.log(chalk.green(`session for "${session.basePath}"\n`));
    rl.prompt();
  },
  session: () => {
    console.log(chalk.white(`session for "${session.basePath}"\n`));
    buffer = [];
    rl.prompt();
  },
  sql: query(),
  total: query(false),
  explain: () => {
    console.log(chalk.white(JSON.stringify(session.diag, null, 2), '\n'));
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
