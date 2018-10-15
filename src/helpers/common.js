'use strict';
const _ = require('lodash');

const appText = (origin, message) => _.filter([origin, message], x => !!x).join(', ');
const accLength = (normAccLen, emerAccLen) => (mode, period) => {
    let percentage = 0;
    switch(mode) {
        case MODE_NORMAL:
            percentage = normAccLen;
            break;
        case MODE_EMERGENCY:
            percentage = emerAccLen;
            break;
        default:
            console.log('Invalid mode "'+mode+'"');
            process.exit();
    }
    
    let len = Math.ceil((percentage / 100) * period);
    
    return Math.max(1, len);
};

const ADVISE_BUY = 1;
const ADVISE_SELL = 2;

const TYPE_LONG = 1;
const TYPE_SHORT = 2;

const MODE_NORMAL = 'n';
const MODE_EMERGENCY = 'e';

const DIR_IN = 'i';
const DIR_OUT = 'o';

const ONE_MINUTE = 60;

const modes = [MODE_NORMAL, MODE_EMERGENCY];
const dirs = [DIR_IN, DIR_OUT];
const types = [TYPE_LONG, TYPE_SHORT];

const DEFAULT_FEE = 0.002;

const _is = t => c => x => x[t] == c;

const openDeal = (flags, ts, price) => {
    let emer = !!(flags.recentlyClosed && flags.closeDisappeared);
    if(flags.openAppeared || emer) {
        if(!flags.hasDeal) {
            return {
                ts_enter: ts,
                price_enter: price,
                emergency_enter: emer
            };
        }
    }

    return null;
};

const closeDeal = (deal, flags, ts, price) => {
    let emer = !!(flags.recentlyOpened && flags.openDisappeared);
    
    if(flags.closeAppeared || emer) {
        return _.extend({}, deal, {
            ts_exit: ts,
            price_exit: price,
            emergency_exit: emer
        });
    }

    return null;
};

const closeLastDeal = (deal, ts, price) => {
    return _.extend({}, deal, {
        ts_exit: ts,
        price_exit: price,
        emergency_exit: false
    });
};

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

module.exports = {
    appText,
    accLength,
    isNormBuy: _is('advise')(ADVISE_BUY),
    isNormSell: _is('advise')(ADVISE_SELL),
    isEmerBuy: _is('state')(ADVISE_BUY),
    isEmerSell: _is('state')(ADVISE_SELL),
    openDeal,
    closeDeal,
    closeLastDeal,
    
    modes,
    dirs,
    types,
    
    MODE_NORMAL,
    MODE_EMERGENCY,
    
    DIR_IN,
    DIR_OUT,
    
    TYPE_LONG,
    TYPE_SHORT,
    
    ONE_MINUTE,
    DEFAULT_FEE
};