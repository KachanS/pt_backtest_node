'use strict';
const cluster = require('cluster');
const _ = require('lodash');
const processor = require('./../processor.js');

if(!cluster.isWorker) {
    console.error('Running worker in master mode');
    process.exit(1001);
}

const JOBS_LIMIT = 50;

const EXIT_CODE_LIMIT  = 1001;
const EXIT_CODE_NO_JOB = 0;

let jobsCompleted = 0;
let queryTimer = null;

function requestJob() {
    process.send({ type: 'idle' });
    queryTimer = setTimeout(() => {
        shutDown(EXIT_CODE_NO_JOB);
    }, 2000);
}

function shutDown(exitCode) {
    exitCode = exitCode || 0;
    process.exit(exitCode);
}

function finishJob(jobId) {
    process.send({ type: 'done', id: jobId });
    jobsCompleted++;
    if(jobsCompleted >= JOBS_LIMIT) {
        shutDown(EXIT_CODE_LIMIT);
    } else {
        requestJob();
    }
};

function doTheJob(jid, pin, pout, start, end, amount) {
    clearTimeout(queryTimer);
    console.time('#'+jid);
    processor
        .calculate(pin, pout, start, end, amount)
        .catch(e => {
            console.error(e);
            console.log('ERROR: ', e);
            
            return null;
        })
        .then(() => {
            console.timeEnd('#'+jid);
            
            finishJob(jid);
        });
}


process.on('message', message => {
    switch(message.type) {
        case 'start':
            let jid = [message.combination[0].join('_'), message.combination[1].join('_')].join(' => ');
            doTheJob(jid, message.combination[0], message.combination[1], message.start, message.end, message.amount);
            break;
        case 'die':
            console.log('Worker shut down after "die" command');
            process.exit(0);
        default:
            console.log('Unknown message');
    }
});

process.on('error', err => {
    console.log(`Worker Error [#${process.pid}]`, err);
});

requestJob();