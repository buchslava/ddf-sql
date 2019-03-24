const chai = require('chai');
const { Parser } = require('flora-sql-parser');
const getRecordFilterFun = require('../src/filter');
const expect = chai.expect;

describe('filters', () => {
  it('geo and time only', done => {
    const sqlQuery = `
        SELECT geo, time, income_mountains
        FROM datapoints
        WHERE time=2018 AND geo='world'
        ORDER BY time`;
    const parser = new Parser();
    const ast = parser.parse(sqlQuery);
    const filterFun = getRecordFilterFun(sqlQuery, ast);

    expect(filterFun({ time: 2018, geo: 'world' })).to.be.true;
    expect(filterFun({ time: 2019, geo: 'world' })).to.be.false;
    expect(filterFun({})).to.be.false;
    expect(filterFun({ time: 2010, geo: 'world' })).to.be.false;
    expect(filterFun({ time: 2018, geo: 'foo' })).to.be.false;

    done();
  });
  it.only('in operator', done => {
    const sqlQuery = `
    SELECT geo, time, income_mountains FROM datapoints 
    WHERE time=2018 AND (geo IN ('world', 'country') 
    OR geo IN ('foo')) ORDER BY time`;
    const parser = new Parser();
    const ast = parser.parse(sqlQuery);
    const filterFun = getRecordFilterFun(sqlQuery, ast);

    done();
  });
});  
