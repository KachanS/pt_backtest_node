'use strict';
const _ = require('lodash');
const { ONE_MINUTE, types, modes, dirs } = require('./common');
const { appText, accLength, isNormBuy, isNormSell, isEmerBuy, isEmerSell, openDeal, closeDeal, closeLastDeal } = require('./common');
const { DIR_IN, DIR_OUT, TYPE_LONG, TYPE_SHORT, MODE_NORMAL, MODE_EMERGENCY, DEFAULT_FEE } = require('./common');

const getAccumulators = (mormAccLen, emerAccLen, period_in, period_out) => {
    let accs = {}, lens = {}, deals = {},
        deal = {}, lastDeal = {}, lastAdv = {};
    _.each(modes, (mode) => {
        accs[mode] = {}, lens[mode] = {};
        _.each(dirs, (dir) => {
            accs[mode][dir] = {};
            lens[mode][dir] = accLength(mormAccLen, emerAccLen)(mode, (dir == DIR_IN)?period_in:period_out);
            _.each(types, (type) => {
                accs[mode][dir][type] = [];
                deals[type] = {};
                deal[type] = null;
                lastDeal[type] = null;
                lastAdv[type] = null;
            });
        });
    });
    
    return {
        accs,
        lens,
        deal,
        deals,
        lastDeal
    };
};

const dateEdges = {
    1: ['date', 7],
    2: ['month', 1],
    3: ['month', 3],
    4: ['month', 6]
};

const ucfirst = s => {
    if(s.length === 0) {
        return s;
    }
    return s.charAt(0).toUpperCase() + s.slice(1);
};

const getDateEdge = ts => edge => {
    let d = new Date(ts*1000);
    const rule = dateEdges[edge];
    const setter = 'set'+ucfirst(rule[0]);
    const getter = 'get'+ucfirst(rule[0]);

    d[setter](d[getter]() - rule[1]);
    return parseInt(d.getTime()/1000);
};

const stats = (deals, type, endTs, amount) => {
    let statistics = {};
    const factor = (type == TYPE_LONG) ? 1 : -1;
    
    _.each(_.keys(dateEdges), edge => {
        let dateEdge = getDateEdge(endTs)(edge);
        let edgeDeals = _.filter(deals, deal => {
            return deal.ts_exit >= dateEdge;
        });

        statistics[edge] = _.reduce(edgeDeals, (s, deal) => {
            let fees = (deal.price_enter + deal.price_exit) * DEFAULT_FEE;
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
                amount: amount
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
    
    return statistics;
};

module.exports = {
    generateDeals: state => {
        let { pin, pout, ain, aout, error, message }
                    = state,
            period_in = pin[3], period_out = pout[3],
            { accs, lens, deal, deals, lastDeal }
                    = getAccumulators(0, 8, period_in, period_out),
            one_in_period = ONE_MINUTE * period_in,
            one_out_period = ONE_MINUTE * period_out,
            cin = null, cout = null,
            cur_interval_in = null, cur_interval_out = null,
            openAppeared, closeAppeared, openDisappeared, closeDisappeared,
            recentlyOpened, recentlyClosed, flags = {}, closedDeal, pts, lastAdv = { ts: 0, close: 0 };
        
        if(error || !_.size(ain) || !_.size(aout)) {
            return _.extend({}, state, {
                error: true,
                message: appText(message, `No data found ${pin} -> ${pout}`)
            });
        }
        
        let cts = parseInt(state.start);
        while(cts <= state.end) {
            cin = ain[cts] || null, cout = aout[cts] || null;
            cur_interval_in = (cts - cts % one_in_period) + one_in_period;
            cur_interval_out = (cts - cts % one_out_period) + one_out_period;
            openAppeared = false, closeAppeared = false;
            openDisappeared = false, closeDisappeared = false;

            if(_.isNull(cin) && _.isNull(cout)) {
                cts += ONE_MINUTE;
                continue;
            }
            
            if(!_.isNull(cin) && lastAdv.ts < cin.ts) {
                lastAdv = _.pick(cin, ['ts', 'close']);
            }
            if(!_.isNull(cout) && lastAdv.ts < cout.ts) {
                lastAdv = _.pick(cout, ['ts', 'close']);
            }
            
            _.each(types, type => {
                recentlyOpened = !!deal[type] && 
                                    (cur_interval_in - deal[type].ts_enter) <= 2 * one_in_period;
                recentlyClosed = !deal[type] && 
                                    !!lastDeal[type] && 
                                    !lastDeal[type].emergency_exit && 
                                    (cur_interval_out - lastDeal[type].ts_exit) <= 2 * one_out_period;

                if(!_.isNull(cin)) {
                    accs[MODE_NORMAL][DIR_IN][type].push(cin);
                    accs[MODE_EMERGENCY][DIR_IN][type].push(cin);
                }
                if(accs[MODE_NORMAL][DIR_IN][type].length > lens[MODE_NORMAL][DIR_IN]) {
                    accs[MODE_NORMAL][DIR_IN][type] = accs[MODE_NORMAL][DIR_IN][type].slice(-lens[MODE_NORMAL][DIR_IN]);
                }
                if(accs[MODE_EMERGENCY][DIR_IN][type].length > lens[MODE_EMERGENCY][DIR_IN]) {
                    accs[MODE_EMERGENCY][DIR_IN][type] = accs[MODE_EMERGENCY][DIR_IN][type].slice(-lens[MODE_EMERGENCY][DIR_IN]);
                }

                if(!_.isNull(cout)) {
                    accs[MODE_NORMAL][DIR_OUT][type].push(cout);
                    accs[MODE_EMERGENCY][DIR_OUT][type].push(cout);
                }
                if(accs[MODE_NORMAL][DIR_OUT][type].length > lens[MODE_NORMAL][DIR_OUT]) {
                    accs[MODE_NORMAL][DIR_OUT][type] = accs[MODE_NORMAL][DIR_OUT][type].slice(-lens[MODE_NORMAL][DIR_OUT]);
                }
                if(accs[MODE_EMERGENCY][DIR_OUT][type].length > lens[MODE_EMERGENCY][DIR_OUT]) {
                    accs[MODE_EMERGENCY][DIR_OUT][type] = accs[MODE_EMERGENCY][DIR_IN][type].slice(-lens[MODE_EMERGENCY][DIR_OUT]);
                }

                switch(type) {
                    case TYPE_LONG:
                        openAppeared = cin && _.every(accs[MODE_NORMAL][DIR_IN][type], isNormBuy);
                        closeAppeared = cout && _.every(accs[MODE_NORMAL][DIR_OUT][type], isNormSell);
                        openDisappeared = _.every(accs[MODE_EMERGENCY][DIR_IN][type], isEmerSell);
                        closeDisappeared = _.every(accs[MODE_EMERGENCY][DIR_OUT][type], isEmerBuy);
                        break;
                    case TYPE_SHORT:
                        openAppeared = cin && _.every(accs[MODE_NORMAL][DIR_IN][type], isNormSell);
                        closeAppeared = cout && _.every(accs[MODE_NORMAL][DIR_OUT][type], isNormBuy);
                        openDisappeared = _.every(accs[MODE_EMERGENCY][DIR_IN][type], isEmerBuy);
                        closeDisappeared = _.every(accs[MODE_EMERGENCY][DIR_OUT][type], isEmerSell);
                        break;
                }

                flags = {
                    recentlyOpened,
                    recentlyClosed,
                    openAppeared,
                    closeAppeared,
                    openDisappeared,
                    closeDisappeared,
                    hasDeal: !!deals[type] && !!deals[type][cur_interval_in]
                };

                if(_.isNull(deal[type]) && !_.isNull(cin)) {
                    deal[type] = openDeal(flags, cts, cin.close);
                } else if(!_.isNull(deal[type]) && !_.isNull(cout)) {
                    closedDeal = closeDeal(deal[type], flags, cts, cout.close);
                    if(!_.isNull(closedDeal)) {
                        pts = closedDeal.ts_enter - closedDeal.ts_enter % one_in_period + one_in_period;
                        deals[type][pts] = closedDeal;
                        lastDeal[type] = closedDeal;
                        deal[type] = null;
                    }
                }
            });

            cts += ONE_MINUTE;
        }
        
        // Close last deals
        _.each(types, type => {
            if(deal[type]) { // Last deal isn\'t closed
                if(deal[type].ts_enter < lastAdv.ts) {
                    deals[type][lastAdv.ts] = closeLastDeal(deal[type], lastAdv.ts, lastAdv.close);
                }
            }
        });
        
        return _.extend({}, state, {
            dlong: {...deals[TYPE_LONG]},
            dshort: {...deals[TYPE_SHORT]}
        });
    },
    calculateStats: state => {
        if(state.error) {
            return state;
        }
        
        return _.extend({}, state, {
            slong: stats(state.dlong, TYPE_LONG, state.end, state.amount),
            sshort: stats(state.dshort, TYPE_LONG, state.end, state.amount)
        });
    }
};