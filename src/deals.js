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

let deltaMap = {
    60: 0,
    240: 1,
    120: 2,
    1440: 0,
    30: 1
};

const SKIPPER_COMBINATION = 3;
const SKIPPER_BACKTEST = 16000;

const FAST_LIST = _.range(2, 31);
const SLOW_LIST = _.range(10, 41);
const SIGNAL_LIST = _.range(2, 21);
const PERIOD_LIST = [30, 60, 120, 240, 1440]

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
    
    if((fast + slow + signal + deltaMap[period]) % SKIPPER_COMBINATION > 0) {
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
                        skipper++;
                        if((skipper % SKIPPER_BACKTEST) == 0) {
                            yield [cin.value, cout.value];
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

function startFork() {
    let combination = g.next();
    if(!combination.done) {
        let w = cluster.fork();
        
        w.send({
            type: 'start',
            combination: combination.value
        });
    } else {
        isDone = true;
    }
}

function checkIsDone() {
    if(isDone && !_.size(cluster.workers)) {
        console.log(`Master is done with ${2*calculations} calculations`);
        console.timeEnd('master');
        process.exit();
    }
}

cluster.on('exit', (worker, code, signal) => {
    console.log(`W#${worker.process.pid} done`);
    calculations++;
    startFork();
    checkIsDone();
});

cluster.on('message', (worker, message) => {
    switch(message.type) {
        case 'done':
            calculations += message.data;
            break;
    }
});

let end = new Date().getTime();
end = parseInt(end/1000); // To seconds
end -= (end % (60*60*24)); // Restrict to current date 00:00:00
let start = end - 180 * (60*60*24);

let startInt = null;
startInt = setInterval(() => {
    if(_.size(cluster.workers) <= numCPUs) {
        let combination = g.next();
        if(!combination.done) {
            let w = cluster.fork();

            w.send({
                type: 'start',
                combination: combination.value,
                start: start,
                end: end,
                amount: 1
            });
        } else {
            clearInterval(startInt);
        }
    } else {
        clearInterval(startInt);
    }
}, 200);

console.time('master');
console.log(`Cluster started successfully`);
