'use strict';
require('dotenv').config();
const mysql = require('mysql');
const _ = require('lodash');
const _defaultDbConfig = {
    connectionLimit: process.env.DB_CONNECTION_LIMIT || 4,
    host: process.env.DB_HOST,
    debug:  false
};

let pools = {
    _default: mysql.createPool(_.extend(_defaultDbConfig, {
        user: process.env.DB_USER,
        password: process.env.DB_PASS,
        database: process.env.DB_NAME
    })),
    bt: mysql.createPool(_.extend(_defaultDbConfig, {
        user: process.env.BT_DB_USER,
        password: process.env.BT_DB_PASS,
        database: process.env.BT_DB_NAME
    }))
};

function queryDb(sql, handler) {
    return queryPool('_default', sql, handler);
};

function queryPool(poolName, sql, handler) {
    if(!pools.hasOwnProperty(poolName)) {
        return Promise.reject({
            message: 'There is no pool with name "'+poolName+'" defined',
            data: {
                name: poolName,
                sql: sql
            }
        });
    }
    
    handler = handler || (x => x);
    return new Promise((resolve, reject) => {
        pools[poolName].query(sql, (err, result) => {
            if(err) {
                reject({
                    message: err.sqlMessage,
                    data: err
                });
            } else {
                resolve(handler(result));
            }
        });
    });
};

exports.queryDb = queryDb;
exports.queryPool = queryPool;
exports.registerPool = (name, config) => {
    pools[name] = mysql.createPool(config || {});
};