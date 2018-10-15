'use strict';
const { queryPool } = require('../service/db');
const _ = require('lodash');
const { TYPE_LONG, TYPE_SHORT } = require('./common');

const SQLs = {
    is_calculated: (p_in, p_out, start, end) => {
        return  'SELECT COUNT(`id`) as c FROM `main_backtest` '+
                'WHERE `buy_fast` = '+p_in[0]+' AND `sell_fast` = '+p_out[0]+' '+
                'AND `buy_slow` = '+p_in[1]+' AND `sell_slow` = '+p_out[1]+' '+
                'AND `buy_signal` = '+p_in[2]+' AND `sell_signal` = '+p_out[2]+' '+
                'AND `buy_period` = '+p_in[3]+' AND `sell_period` = '+p_out[3]+' '+
                'AND `ts_end` = '+end+' AND `active` > 0';
    },
    fetch: (params, start, end) => {
        return  'SELECT `ts`, `close`, `advise`, `state` '+
                'FROM `a_'+params[0]+'_'+params[2]+'_'+params[3]+'` '+
                'WHERE `p_slow` = '+params[1]+' '+
                'AND `ts` BETWEEN '+start+' AND '+end+' '+
                'ORDER BY `ts` ASC';
    },
    insert_backtest: (state, stats, type) => {
        const { pin, pout, start, end, amount } = state;
        return 'INSERT INTO `main_backtest` VALUES('+
                'NULL, 1, 0, NOW(), NOW(), "|main.backtest|", 0, 3, '+
                '\''+JSON.stringify({ statistics: stats })+'\', '+
                '"'+(type == TYPE_LONG?'Long':'Short')+': In '+pin.join('_')+' -> Out '+pout.join('_')+'", '+
                pin.join(', ')+', '+pout.join(', ')+', '+
                start+', '+end+', '+
                stats[1].profit+', '+stats[2].profit+', '+stats[3].profit+', '+stats[4].profit+', '+
                type+', 1, '+amount+' '+
                ')'
                ;
    },
    find_backtest: (pin, pout, type) => {
        return 'SELECT `id` FROM `main_backtest` '+
                'WHERE `buy_fast` = '+pin[0]+' AND `sell_fast` = '+pout[0]+' '+
                'AND `buy_slow` = '+pin[1]+' AND `sell_slow` = '+pout[1]+' '+
                'AND `buy_signal` = '+pin[2]+' AND `sell_signal` = '+pout[2]+' '+
                'AND `buy_period` = '+pin[3]+' AND `sell_period` = '+pout[3]+' '+
                'AND `type` = '+type+' '+
                'ORDER BY `ts_end`'
                ;
    },
    update_backtest: (state, stats, id) => {
        const { start, end } = state;
        return 'UPDATE `main_backtest` SET '+
                'data = \''+JSON.stringify({ statistics: stats })+'\', '+
                'ts_start = '+start+', ts_end='+end+', '+
                'total_week = '+stats[1].profit+', '+
                'total_month1 = '+stats[2].profit+', '+
                'total_month3 = '+stats[3].profit+', '+
                'total_month6 = '+stats[4].profit+' '+
                'WHERE `id` = '+id
                ;
    }
};

let verbose = false;

let fetchReducer = raw => {
    return {
        error: false,
        message: '',
        data: _.reduce(raw, (acc, item) => {
            acc[item.ts] = {
                ts: item.ts,
                close: item.close,
                advise: item.advise,
                state: item.state
            };
            return acc;
        }, {})
    };
};

let fetch = (params, start, end) => {
    let sql = SQLs.fetch(params, start, end);
    return queryPool('_default', sql, fetchReducer)
            .catch(err => {
                if(verbose) {
                    console.log(`[DB Error] When fetching advises for params ${params}`);
                    console.log(err.message);
                }
                return {
                    error: true,
                    message: err.message,
                    data: {}
                };
            });
};

const fetchInAdvises = state => {
    let { pin, start, end, error, message, ain } = state;

    if(error) {
        return state;
    }

    return fetch(pin, start, end)
                .then(advData => {
                    error |= advData.error;
                    message = _.filter([message, advData.message], x => !!x).join(', ');
                    ain = advData.data;
                })
                .then(() => _.extend({}, state, {
                    error,
                    message,
                    ain
                }));
};

const fetchOutAdvises = state => {
    let { pin, pout, start, end, error, message, ain, aout } = state;
    
    if(error) {
        return state;
    }

    if(pin.join('.') == pout.join('.')) {
        aout = {...ain};

        return _.extend({}, state, {
            aout
        });
    } else {
        return fetch(pout, start, end)
                    .then(advData => {
                        error |= advData.error;
                        message = _.filter([message, advData.message], x => !!x).join(', ');
                        aout = advData.data;
                    })
                    .then(() => _.extend({}, state, {
                        error,
                        message,
                        aout
                    }));
    }
};

const saveBt = (key, type) => state => {
    const { pin, pout } = state;
    const stats = state[key] || {};
    
    if(state.error) {
        return state;
    }
    
    if(!_.size(stats)) {
        return _.extend({}, state, {
            error: true,
            message: 'No data'
        });
    }
    
    return queryPool('bt', SQLs.find_backtest(pin, pout, type))
            .catch(err => {
                console.log('[DB ERROR] On search. '+err.message);
                return [];
            })
            .then(res => {
                if(res.length && res[0] && res[0].id) {
                    return queryPool('bt', SQLs.update_backtest(state, stats, res[0].id));
                } else {
                    return queryPool('bt', SQLs.insert_backtest(state, stats, type));
                }
            })
            .catch(err => {
                console.log('[DB ERROR] On insert/update. '+err.message);
                return null;
            })
            .then(() => {
                return _.extend({}, state);
            });
};

const saveLong = saveBt('slong', TYPE_LONG);
const saveShort = saveBt('slong', TYPE_SHORT);

module.exports = {
    notCalculated: state => {
        let { pin, pout, start, end } = state,
            sql = SQLs.is_calculated(pin, pout, start, end);
        
        return queryPool('bt', sql)
                    .then(data => {
                        if(data[0].c >= 2) {
                            // Generate rejection to be cacthed ASAP
                            return Promise.reject(_.extend({}, state, {
                                error: true,
                                message: 'Backtest already calculated'
                            }));
                        } else {
                            return Promise.resolve(_.extend({}, state, {
                                error: false
                            }));
                        }
                    });
    },
    fetchDeals: state => Promise.resolve(state)
                            .then(fetchInAdvises)
                            .then(fetchOutAdvises)
    ,
    saveBacktest: state => Promise.resolve(state)
                                    .then(saveLong)
                                    .then(saveShort),
    insert: () => {},
    verbose: newValue => { verbose = !!newValue; }
};