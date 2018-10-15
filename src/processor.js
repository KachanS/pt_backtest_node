'use strict';
const _ = require('lodash');
const { queryPool } = require('./service/db');
const dbTools = require('./helpers/db');
const calcTools = require('./helpers/calc');

const DEBUG = false;

const logger = processor => {
    processor = (typeof processor === 'function')?processor:console.log;
    return state => {
        let data;
        if(DEBUG) {
            data = processor(state);
            if(_.isArray(data)) {
                console.log.apply(null, data);
            }
        }
        return state;
    };
};

const calculate = (p_in, p_out, start, end, amount) => {
    // Check if data is calculated
    let state = {
        error: false,
        message: '',
        pin: p_in,
        pout: p_out,
        start: start,
        end: end,
        amount: amount,
        ain: {},
        aout: {}
    };
    
    return Promise.resolve(state)
            .then(dbTools.notCalculated)
            .then(logger(s => ['Not calculated', s.error, s.message, _.keys(s)]))
            .then(s => { console.time(`db ${s.pin} -> ${s.pout}`); return s; })
            .then(dbTools.fetchDeals)
            .then(s => { console.timeEnd(`db ${s.pin} -> ${s.pout}`); return s; })
            .then(logger(s => ['Deals fetched', s.error, s.message, _.keys(s)]))
            .then(s => { console.time(`deals ${s.pin} -> ${s.pout}`); return s; })
            .then(calcTools.generateDeals)
            .then(s => { console.timeEnd(`deals ${s.pin} -> ${s.pout}`); return s; })
            .then(s => { console.time(`stats ${s.pin} -> ${s.pout}`); return s; })
            .then(calcTools.calculateStats)
            .then(s => { console.timeEnd(`stats ${s.pin} -> ${s.pout}`); return s; })
            .then(logger(s => [_.omit(s.slong[4], ['calcs']), _.omit(s.sshort[4], ['calcs'])]))
            .then(s => { console.time(`save ${s.pin} -> ${s.pout}`); return s; })
            .then(dbTools.saveBacktest)
            .then(s => { console.timeEnd(`save ${s.pin} -> ${s.pout}`); return s; })
            .catch(err => { console.log('Error: %s', err.message); return Promise.resolve(state); })
            ;
};

module.exports.calculate = calculate;