var Table = require('easy-table')
const Session = require('./session');

/*
SELECT concept, concept_type FROM concepts`
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
  sql: async () => {
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
      const result = await session.runSQL(sql)
      console.timeEnd(metricLabel);
      console.log(Table.print(result));
      console.log(`${result.length} were selected...`);  
    } catch (e) {
      console.log(e);
    } finally {
      buffer = [];
      rl.prompt();  
    }
  },
  q: () => {
    rl.close();
  }
};

rl.on('line', input => {
  input = input.toLowerCase();

  if (input in commands) {
    commands[input]();
  } else {
    buffer.push(input);
  }
});
