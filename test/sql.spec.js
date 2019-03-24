const chai = require('chai');
const path = require('path');
const Session = require('../src/session');
const expect = chai.expect;

const sgPath = path.resolve('..', 'sg');

describe('queries', () => {
  describe('happy flow', () => {
    describe('concepts', () => {
      it('concept and concept_type for systema_globalis', async () => {
        const session = new Session(sgPath);
        const result = await session.runSQL(`SELECT concept, concept_type FROM concepts`)

        expect(result.length).to.be.equal(590);
      });
    });
    describe('datapoints', () => {
      it('income_mountains for year 2018 or 2019', async () => {
        const sqlQuery = `
        SELECT geo, time, income_mountains
        FROM datapoints
        WHERE (time=2018 OR time=2019) AND geo='world'
        ORDER BY time`;
        const session = new Session(sgPath);
        const result = await session.runSQL(sqlQuery)

        expect(result.length).to.be.equal(2);
      });
    });
  });
});
