'use strict';
const _ = require('lodash');
const { queryPool } = require('./service/db');
const SQLs = {
    is_calculated: function(p_in, p_out, start, end) {
        return  'SELECT COUNT(`id`) as c FROM `main_backtest` '+
                'WHERE `buy_fast` = '+p_in[0]+' AND `sell_fast` = '+p_out[0]+' '+
                'AND `buy_slow` = '+p_in[1]+' AND `sell_slow` = '+p_out[1]+' '+
                'AND `buy_signal` = '+p_in[2]+' AND `sell_signal` = '+p_out[2]+' '+
                'AND `buy_period` = '+p_in[3]+' AND `sell_period` = '+p_out[3]+' '+
                'AND `ts_end` = '+end+' AND `active` > 0';
    },
    fetch_advises: function(fast, slow, signal, period, start, end) {
        return  'SELECT `ts`, `close`, `advise`, `state` '+
                'FROM `a_'+fast+'_'+signal+'_'+period+'` '+
                'WHERE `p_slow` = '+slow+' '+
                'AND `ts` BETWEEN '+start+' AND '+end+' '+
                'ORDER BY `ts` ASC';
    },
    delete_backtest: function(state, type) {
        let p_in = state.pin,
            p_out = state.pout;
        return  'DELETE FROM `main_backtest` '+
                'WHERE `buy_fast` = '+p_in[0]+' AND `sell_fast` = '+p_out[0]+' '+
                'AND `buy_slow` = '+p_in[1]+' AND `sell_slow` = '+p_out[1]+' '+
                'AND `buy_signal` = '+p_in[2]+' AND `sell_signal` = '+p_out[2]+' '+
                'AND `buy_period` = '+p_in[3]+' AND `sell_period` = '+p_out[3]+' '+
                'AND `type` = '+type+' ';
    },
    insert_backtest: function(state, type) {
        let p_in = state.pin,
            p_out = state.pout,
            stats = state.stats[''+type];
        return 'INSERT INTO `main_backtest` VALUES('+
                'NULL, 1, 0, NOW(), NOW(), "|main.backtest|", 0, 3, '+
                '\''+JSON.stringify({ statistics: stats })+'\', '+
                '"'+(type == TYPE_LONG?'Long':'Short')+': In '+p_in.join('_')+' -> Out '+p_out.join('_')+'", '+
                p_in.join(', ')+', '+p_out.join(', ')+', '+
                state.start+', '+state.end+', '+
                stats[1].profit+', '+stats[2].profit+', '+stats[3].profit+', '+stats[4].profit+', '+
                type+', 1, '+state.amount+' '+
                ')'
                ;
    }
};

const ADVISE_BUY = 1;
const ADVISE_SELL = 2;

const TYPE_LONG = 1;
const TYPE_SHORT = 2;

const MODE_NORMAL = 'n';
const MODE_EMERGENCY = 'e';

const DIR_IN = 'i';
const DIR_OUT = 'o';

const NORM_ACC_LEN = 0;
const EMER_ACC_LEN = 8;

const ONE_MINUTE = 60;

const modes = [MODE_NORMAL, MODE_EMERGENCY];
const dirs = [DIR_IN, DIR_OUT];
const types = [TYPE_LONG, TYPE_SHORT];

const FEE = 0.002;

if(!String.prototype.ucfirst) {
    Object.assign(String.prototype, {
        ucfirst() {
            if(this.length) {
                return this.charAt(0).toUpperCase() + this.slice(1);
            }
            return this;
        }
    });
}

let checkCalculated = (p_in, p_out, start, end, amount) => {
    return new Promise((resolve, reject) => {
        queryPool('bt', SQLs.is_calculated(p_in, p_out, start, end))
            .then(data => {
                if(data[0].c >= 2) {
                    reject({
                        message: 'Already calculated',
                        data: {}
                    });
                } else {
                    resolve({
                        pin: p_in,
                        pout: p_out,
                        start: start,
                        end: end,
                        amount: amount
                    });
                }
            })
            .catch(err => {
                reject({
                    message: 'Error checking calculation status',
                    data: err
                });
            });
    });
};

let calcAccLength = (mode, period) => {
    let percentage = 0;
    switch(mode) {
        case MODE_NORMAL:
            percentage = NORM_ACC_LEN;
            break;
        case MODE_EMERGENCY:
            percentage = EMER_ACC_LEN;
            break;
        default:
            console.error('Mode: '+mode);
            console.log('Invalid mode');
            process.exit();
    }
    
    let len = Math.ceil((percentage / 100) * period);
    
    return Math.max(1, len);
};

//let _is = t => c => x => x[t] == c
let isNormBuy = x => x.advise == ADVISE_BUY;
let isNormSell = x => x.advise == ADVISE_SELL;
let isEmerBuy = x => x.state == ADVISE_BUY;
let isEmerSell = x => x.state == ADVISE_SELL;

let c = 0;
let fetchProxy = sql => {
    return queryPool('_default', sql, res => _.reduce(res, (acc, item) => {
        acc[item.ts] = {
            ts: item.ts,
            close: item.close,
            advise: item.advise,
            state: item.state
        };
        return acc;
    }, {}));
};

let insertProxy = (state, type) => {
    return queryPool('bt', SQLs.delete_backtest(state, type))
                .then(queryPool('bt', SQLs.insert_backtest(state, type)))
                ;
};

let cStarted = 0, cEnded = 0;

let startDeal = (flags, ts, price) => {
    let emer = !!(flags.recentlyClosed && flags.closeDisappeared);
    //console.log('Starting DEAL', flags, ts, price);
    if(flags.openAppeared || emer) {
        if(!flags.hasDeal) {
            cStarted++;
            //if(cStarted > 1) {
            //if(emer) {
            //    console.log('StartDEAL', flags, ts, price);
            //}
            //}
            return {
                ts_enter: ts,
                price_enter: price,
                emergency_enter: emer
            };
        }
    }

    return null;
};

let closeDeal = (deal, flags, ts, price) => {
    let emer = !!(flags.recentlyOpened && flags.openDisappeared);
    
    if(flags.closeAppeared || emer) {
        cEnded++;
        //console.log('CloseDEAL', deal, flags, ts, price);
        return _.extend({}, deal, {
            ts_exit: ts,
            price_exit: price,
            emergency_exit: emer
        });
    }

    return null;
};

let pdate = ts => new Date(ts*1000).toUTCString();

let stats = (state, deals, type) => new Promise((resolve, reject) => {
    let endTs = state.end;
    let statistics = {};
    let factor = (type == TYPE_LONG) ? 1 : -1;
    let dateEdges = {
        1: ['date', 7],
        2: ['month', 1],
        3: ['month', 3],
        4: ['month', 6]
    };
    let getDateEdge = edge => {
        let d = new Date(endTs*1000);
        let rule = dateEdges[edge];
        let setter = 'set'+ucfirst(rule[0]);
        let getter = 'get'+ucfirst(rule[0]);
        
        d[setter](d[getter]() - rule[1]);
        return d;
    };
    
    _.each(_.keys(dateEdges), edge => {
        let dateEdge = getDateEdge(edge),
            dateEdgeTs = parseInt(dateEdge.getTime()/1000);
        let edgeDeals = _.filter(deals, deal => {
            return deal.ts_exit >= dateEdgeTs;
        });
        
        statistics[edge] = _.reduce(edgeDeals, (s, deal) => {
            let fees = (deal.price_enter + deal.price_exit) * FEE;
            let delta = (deal.price_exit - deal.price_enter) * factor;
            
            if(delta > 0) {
                s.wins += 1;
                s.totalWins += delta;
            } else {
                s.losses += 1;
                s.totalLosses += delta;
            }
            s.trades += 1;
            s.fees += fees;
            s.total += delta;
            s.profit += delta - fees;
            
            s.calcs.push(_.extend({}, deal, {
                ts: deal.ts_exit,
                fees: fees,
                delta: delta,
                interval: edge,
                amount: state.amount
            }));
            
            return s;
        }, {
            fees: 0.0,
            profit: 0.0,
            total: 0.0,
            totalWins: 0.0,
            totalLosses: 0.0,
            wins: 0,
            losses: 0,
            trades: 0,
            calcs: []
        });
    });
    
    resolve(statistics);
});

let calculateStatistics = state => {
    return new Promise((resolve, reject) => {
        let fast_in = state.pin[0],
            slow_in = state.pin[1],
            signal_in = state.pin[2],
            period_in = state.pin[3],
            fast_out = state.pout[0],
            slow_out = state.pout[1],
            signal_out = state.pout[2],
            period_out = state.pout[3];
        let k = state.pin.join('_')+' => '+state.pout.join('_');
        console.time(k+':db');
        let sqlPromise = null;
        if(state.pin== state.pout) {
            sqlPromise = fetchProxy(SQLs.fetch_advises(fast_in, slow_in, signal_in, period_in, state.start, state.end))
                    .catch(e => {
                        return _.extend({}, state, {
                            error: true,
                            errorMessage: e.message
                        });
                    })
                    .then(data => {
                        if(data.hasOwnProperty('error') && data.error) {
                            return _.extend({}, state, {
                                adv_in: [],
                                adv_out: []
                            });
                        } else {
                            return {
                                adv_in: data,
                                adv_out: data
                            };
                        }
                    });
        } else {
            sqlPromise = Promise.all([
                fetchProxy(SQLs.fetch_advises(fast_in, slow_in, signal_in, period_in, state.start, state.end)),
                fetchProxy(SQLs.fetch_advises(fast_out, slow_out, signal_out, period_out, state.start, state.end))
            ]).catch(e => {
                return _.extend({}, state, {
                    error: true,
                    errorMessage: e.message
                });
            }).then(data => {
                if(data.hasOwnProperty('error') && data.error) {
                    return _.extend({}, state, {
                        adv_in: [],
                        adv_out: []
                    });
                } else {
                    return {
                        adv_in: data[0],
                        adv_out: data[1]
                    };
                }
            });
        }
        
        return sqlPromise.then(st => {
            if(st.error || !_.size(st.adv_in) || !_.size(st.adv_out)) {
                return _.extend({}, st, {
                    error: true,
                    errorMessage: 'Can not fetch advises'
                });
            }
            
            let adv_in = st.adv_in, 
                adv_out = st.adv_out,
                deals = {}, deal = {},
                lastDeal = {}, accs = {},
                lens = {};
            console.timeEnd(k+':db');
            
            for(let midx in modes) {
                accs[modes[midx]] = {};
                lens[modes[midx]] = {};
                for(let didx in dirs) {
                    accs[modes[midx]][dirs[didx]] = {};
                    lens[modes[midx]][dirs[didx]] = calcAccLength(modes[midx], (dirs[didx] == DIR_IN)?period_in:period_out);
                    for(let tidx in types) {
                        accs[modes[midx]][dirs[didx]][types[tidx]] = [];
                        deals[types[tidx]] = {};
                        deal[types[tidx]] = null;
                        lastDeal[types[tidx]] = null;
                    }
                }
            }
            
            console.time(k+':calc');
            let cts = parseInt(state.start);
            while(cts <= state.end) {
                let one_in_period = ONE_MINUTE * period_in,
                    one_out_period = ONE_MINUTE * period_out,
                    cin_advise = adv_in[cts] || null,
                    cout_advise = adv_out[cts] || null,
                    cur_interval_in = (cts - cts % one_in_period) + one_in_period,
                    cur_interval_out = (cts - cts % one_out_period) + one_out_period,
                    open_appeared = false,
                    close_appeared = false,
                    open_disappeared = false,
                    close_disappeared = false;
                
                if(_.isNull(cin_advise) && _.isNull(cout_advise)) {
                    cts += ONE_MINUTE;
                    continue;
                }
                
                _.each([TYPE_LONG, TYPE_SHORT], type => {
                    let recently_opened = !!deal[type] && 
                                            (cur_interval_in - deal[type].ts_enter) <= 2 * one_in_period,
                        recently_closed = !deal[type] && 
                                            !!lastDeal[type] && 
                                            !lastDeal[type].emergency_exit && 
                                            (cur_interval_out - lastDeal[type].ts_exit) <= 2 * one_out_period;
                    
                    if(!_.isNull(cin_advise)) {
                        accs[MODE_NORMAL][DIR_IN][type].push(cin_advise);
                        accs[MODE_EMERGENCY][DIR_IN][type].push(cin_advise);
                    }
                    if(accs[MODE_NORMAL][DIR_IN][type].length > lens[MODE_NORMAL][DIR_IN]) {
                        accs[MODE_NORMAL][DIR_IN][type] = accs[MODE_NORMAL][DIR_IN][type].slice(-lens[MODE_NORMAL][DIR_IN]);
                    }
                    if(accs[MODE_EMERGENCY][DIR_IN][type].length > lens[MODE_EMERGENCY][DIR_IN]) {
                        accs[MODE_EMERGENCY][DIR_IN][type] = accs[MODE_EMERGENCY][DIR_IN][type].slice(-lens[MODE_EMERGENCY][DIR_IN]);
                    }
                    
                    if(!_.isNull(cout_advise)) {
                        accs[MODE_NORMAL][DIR_OUT][type].push(cout_advise);
                        accs[MODE_EMERGENCY][DIR_OUT][type].push(cout_advise);
                    }
                    if(accs[MODE_NORMAL][DIR_OUT][type].length > lens[MODE_NORMAL][DIR_OUT]) {
                        accs[MODE_NORMAL][DIR_OUT][type] = accs[MODE_NORMAL][DIR_OUT][type].slice(-lens[MODE_NORMAL][DIR_OUT]);
                    }
                    if(accs[MODE_EMERGENCY][DIR_OUT][type].length > lens[MODE_EMERGENCY][DIR_OUT]) {
                        accs[MODE_EMERGENCY][DIR_OUT][type] = accs[MODE_EMERGENCY][DIR_IN][type].slice(-lens[MODE_EMERGENCY][DIR_OUT]);
                    }
                    
                    switch(type) {
                        case TYPE_LONG:
                            open_appeared = cin_advise && _.every(accs[MODE_NORMAL][DIR_IN][type], isNormBuy);
                            close_appeared = cout_advise && _.every(accs[MODE_NORMAL][DIR_OUT][type], isNormSell);
                            open_disappeared = _.every(accs[MODE_EMERGENCY][DIR_IN][type], isEmerSell);
                            close_disappeared = _.every(accs[MODE_EMERGENCY][DIR_OUT][type], isEmerBuy);
                            break;
                        case TYPE_SHORT:
                            open_appeared = cin_advise && _.every(accs[MODE_NORMAL][DIR_IN][type], isNormSell);
                            close_appeared = cout_advise && _.every(accs[MODE_NORMAL][DIR_OUT][type], isNormBuy);
                            open_disappeared = _.every(accs[MODE_EMERGENCY][DIR_IN][type], isEmerBuy);
                            close_disappeared = _.every(accs[MODE_EMERGENCY][DIR_OUT][type], isEmerSell);
                            break;
                    }
                    
                    let flags = {
                        recentlyOpened: recently_opened,
                        recentlyClosed: recently_closed,
                        openAppeared: open_appeared,
                        closeAppeared: close_appeared,
                        openDisappeared: open_disappeared,
                        closeDisappeared: close_disappeared,
                        hasDeal: !!deals[type] && !!deals[type][cur_interval_in]
                    };
                    
                    if(_.isNull(deal[type]) && !_.isNull(cin_advise)) {
                        deal[type] = startDeal(flags, cts, cin_advise.close);
                    } else if(!_.isNull(deal[type]) && !_.isNull(cout_advise)) {
                        let closedDeal = closeDeal(deal[type], flags, cts, cout_advise.close);
                        if(!_.isNull(closedDeal)) {
                            let pts = closedDeal.ts_enter - closedDeal.ts_enter % one_in_period + one_in_period;
                            deals[type][pts] = closedDeal;
                            lastDeal[type] = closedDeal;
                            deal[type] = null;
                        }
                    }
                });
                
                cts += ONE_MINUTE;
            }
            
            // @TODO: Add last Advise close last deal logic
            console.timeEnd(k+':calc');
            
            let statPromises = [];
            for(let tidx in types) {
                statPromises.push(stats(state, deals[types[tidx]], types[tidx]));
            }
            
            return Promise.all(statPromises).then(res => {
                let statistics = {};
                for(let ridx in res) {
                    statistics[types[ridx]] = res[ridx];
                }
                return _.extend({}, state, {
                    stats: statistics
                });
            });
        })
        .then(state => {
            // Save data to DB
            if(!state.error) {
                return Promise.all([
                    insertProxy(state, TYPE_LONG),
                    insertProxy(state, TYPE_SHORT)
                ])
                .then(() => {
                    resolve(state);
                });
            } else {
                resolve(state);
            }
        });
    });
};

let calculate = (p_in, p_out, start, end, amount) => {
    // Check if data is calculated
    return checkCalculated(p_in, p_out, start, end, amount)
            .then(calculateStatistics)
            .catch(error => {
                return {
                    result: false,
                    message: error.message,
                    calculation: null,
                    error: error
                };
            });
};

module.exports.calculate = calculate;
// a_10_14_60 WHERE p_slow = 27
// a_20_19_60 WHERE p_slow = 36
/*calculate([10, 27, 14, 60], [20, 36, 19, 60], 1521936000, 1537488000).catch(e => {
    console.log('Error', e);
});*/
                    
let ucfirst = (s) => {
    if(s.length === 0) {
        return s;
    }
    return s.charAt(0).toUpperCase() + s.slice(1);
};