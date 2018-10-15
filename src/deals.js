'use strict';
const cluster = require('cluster');
const _ = require('lodash');
const numCPUs = require('os').cpus().length;

cluster.setupMaster({
    exec: 'src/worker/deals.js'
});

if(!cluster.isMaster) {
    console.error('Running master in worker mode');
    process.exit(1001);
}

const SKIPPER_COMBINATION = 3;
const SKIPPER_BACKTEST = 1;//16000;

const FAST_LIST = _.range(2, 31);
const SLOW_LIST = _.range(10, 41);
const SIGNAL_LIST = _.range(2, 21);
const PERIOD_LIST = [30, 60, 120, 240, 1440];

function* combinations() {
    for(let pidx in PERIOD_LIST) {
        for(let fidx in FAST_LIST) {
            for(let sidx in SLOW_LIST) {
                for(let siidx in SIGNAL_LIST) {
                    yield [FAST_LIST[fidx], SLOW_LIST[sidx], SIGNAL_LIST[siidx], PERIOD_LIST[pidx]];
                }
            }
        }
    }
}

function isCombinationValid(combination) {
    let fast = parseInt(combination[0]),
        slow = parseInt(combination[1]),
        signal = parseInt(combination[2]),
        period = parseInt(combination[3]);
    
    if(fast + 2 > slow) {
        return false;
    }
    
    if((fast + slow + signal) % SKIPPER_COMBINATION > 0) {
        return false;
    }
    
    return true;
}

function* gen() {
    let cing = combinations(),
        cin = cing.next(),
        coutg, cout,
        skipper = 0;
    
    while(!cin.done) {
        if(isCombinationValid(cin.value)) {
            coutg = combinations();
            cout = coutg.next();
            while(!cout.done) {
                if(isCombinationValid(cout.value)) {
                    if(cin.value[3] >= cout.value[3]) {
                        if(cin.value[0] == cout.value[0] && cin.value[1] == cout.value[1] && cin.value[2] == cout.value[2]) {
                            skipper++;
                            if((skipper % SKIPPER_BACKTEST) == 0) {
                                yield [cin.value, cout.value];
                            }
                        }
                    }
                }
                
                cout = coutg.next();
            }
        }
        
        cin = cing.next();
    }
}

let g = gen();
let isDone = false;
let calculations = 0;

let end = new Date().getTime();
end = parseInt(end/1000); // To seconds
end -= (end % (60*60*24)); // Restrict to current date 00:00:00
let start = end - 180 * (60*60*24);

let skipedCombinations = [];
let calculatedCount = 0;

cluster.on('message', (worker, message) => {
    switch(message.type) {
        case 'idle':
            let combination = g.next();
            if(!combination.done) {
                try {
                    worker.send({
                        type: 'start',
                        combination: combination.value,
                        start: start,
                        end: end,
                        amount: 1
                    });
                } catch(e) {
                    console.log('SEND Error', e);
                    skipedCombinations.push(combination.value);
                }
            } else {
                worker.send({ type: 'die' });
            }
            break;
        case 'done':
            calculatedCount++;
            break;
    }
});

cluster.on('exit', (worker, code, signal) => {
    if(code) {
        console.log('Restarting fork. Signal: %s, Code: %s', signal, code);
        cluster.fork();
    } else {
        //console.log('Worker %d died (%s)', worker.process.pid, signal || code);
        if(!_.size(cluster.workers)) {
            console.log('Done');
            console.log('Skipped: ');
            console.log(skipedCombinations);
            console.log(_.size(skipedCombinations));
            console.log('Calculated: ', calculatedCount);
            console.timeEnd('Total time');
        }
    }
});

cluster.on('error', (worker, err) => {
    console.log(`Error [#${worker.process.pid}]`, err);
});

console.time('Total time');
_.each(_.range(0, numCPUs), i => {
    setTimeout(() => {
        cluster.fork();
    }, i*200);
});