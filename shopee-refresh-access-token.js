const env = require('./env').env;
const config = require('../config')[env];
const mysql = require('mysql');
const pool = mysql.createPool(config.database);
const axios = require('axios');
const crypto = require('crypto');
const dateformat = require('dateformat');

let info = {
    tokens: [],
}

const execute = (sql, callback, data = {}) => {

    pool.getConnection((err, connection) => {
        if (err) throw err;

        connection.query(sql, data, (err, rows) => {
            connection.release();

            if (err) {
                throw err;
            } else {
                callback(err, rows);

            }
        });
    });
}

const closing = () => {
    pool.end();
}

const getShopeeSync = () => {

    return new Promise((resolve, reject) => {

        execute(`SELECT * FROM app_shopee_sync WHERE is_item = 1`, (err, rows) => {

            if (err) throw err;
            info.tokens = info.tokens.concat(rows);
            resolve();
        })
    })
};

const getNewTokenLoop = () => {

    return new Promise((resolve, reject) => {

        let loop = 0;

        const goway = () => {
            info.tokens.length === loop ? resolve() : generateToken(info.tokens[loop++], goway);
        }
        generateToken(info.tokens[loop++], goway);

    })
};

const generateToken = (info, callback) => {

    const host = 'https://partner.shopeemobile.com';
    const path = "/api/v2/auth/access_token/get";

    const refresh_token = info.refresh_token;
    const shop_id = Number(info.shop_id);
    const partner_key = info.partner_key;
    const partner_id = Number(info.partner_id);

    const timestamp = new Date().getTime();
    const convert = Number((timestamp.toString()).substr(0, 10));

    let stringformat = `${partner_id}${path}${convert}`;
    stringformat = stringformat.toString();

    const sign = crypto.createHmac('sha256', partner_key).update(stringformat).digest('hex').toLowerCase();

    axios({
        method: 'POST',
        url: `${host}${path}?partner_id=${partner_id}&timestamp=${convert}&sign=${sign}`,
        headers: {
            "Content-Type": "application/json"
        },
        data: {
            refresh_token: refresh_token,
            partner_id: partner_id,
            shop_id: shop_id,
        }
    })
        .then((response) => {

            const sync_id = info.sync_id;
            const access_token = response.data.access_token;
            const refresh_token = response.data.refresh_token;
            const expires_at_time = dateformat(new Date(new Date().setHours(new Date().getHours() + 4)), 'yyyy-mm-dd HH:MM:ss')
            const refresh_token_expires_at = dateformat(new Date(new Date().setDate(new Date().getDate() + 30)), 'yyyy-mm-dd HH:MM:ss')

            execute(`UPDATE app_shopee_sync 
                SET access_token="${access_token}",
                expires_at_time="${expires_at_time}",
                refresh_token="${refresh_token}",
                refresh_token_expires_at = "${refresh_token_expires_at}"
                WHERE sync_id = ${sync_id}`, (err, rows) => {

                if (err) {
                    throw err;
                } else {
                    callback();
                }
            })

        })
        .catch((err) => {
            closing();
            console.log(err)
        })
};

const getNewAccessToken = async () => {

    try {
        console.log(new Date() + '시작');
        await getShopeeSync();
        await getNewTokenLoop();
        console.log(new Date() + '종료');
        pool.end();

    } catch (err) {
        console.log(err);
    }
}

getNewAccessToken();