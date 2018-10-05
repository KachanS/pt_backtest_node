'use strict';
const cluster = require('cluster');
const _ = require('lodash');
const processor = require('./../processor.js');

if(!cluster.isWorker) {
    console.error('Running worker in master mode');
    process.exit(1001);
}

console.log(`Worker ${process.pid} started`);

process.on('message', message => {
    switch(message.type) {
        case 'start':
            console.time('worker');
            processor
                .calculate(message.combination[0], message.combination[1], message.start, message.end, message.amount)
                .catch(e => {
                    console.error(e);
                    console.log('ERROR: ', e);
                    console.timeEnd('worker');
                    process.exit(0);
                })
                .then(state => {
                    console.log('Ended ', message.combination[0], message.combination[1]);
                    console.timeEnd('worker');
                    process.exit(0);
                });
            break;
        default:
            console.log('Unknown message');
    }
});