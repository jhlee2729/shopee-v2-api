const env = require('./env').env;
const config = require('../config')[env];
const mysql = require('mysql');
const pool = mysql.createPool(config.database);
const axios = require('axios');
const crypto = require('crypto');
const dateformat = require('dateformat');

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

        execute(`SELECT
            main_account_id,
            merchant_id,
            partner_id,
            partner_key,
            access_token,
            expires_at_time,
            refresh_token,
            refresh_token_expires_at
                FROM app_shopee_sync
                GROUP BY main_account_id,merchant_id,partner_id,partner_key,access_token,expires_at_time,refresh_token,refresh_token_expires_at`, (err, rows) => {

            if (err) {
                throw err
            } else {

                let count = rows.length;
                let check = 0;

                const goway = () => {

                    if (count != check) {
                        refreshAccessToken(rows[check++], goway, check == count);
                    } else {
                        resolve();
                    }
                }

                goway();
            };

        })
    })
};

const refreshAccessToken = (syncData, callback, bool) => {
    return new Promise((resolve, reject) => {

        const host = 'https://partner.shopeemobile.com';
        const path = "/api/v2/auth/access_token/get";

        const refresh_token = syncData.refresh_token;
        const partner_id = Number(syncData.partner_id);
        const merchant_id = Number(syncData.merchant_id);
        const main_account_id = Number(syncData.main_account_id);
        const key = syncData.partner_key;

        const timestamp = new Date().getTime();
        const convert = Number((timestamp.toString()).substr(0, 10));

        let stringformat = `${partner_id}${path}${convert}`;
        stringformat = stringformat.toString();

        const sign = crypto.createHmac('sha256', key).update(stringformat).digest('hex').toLowerCase();

        axios({
            method: 'POST',
            url: `${host}${path}?partner_id=${partner_id}&timestamp=${convert}&sign=${sign}`,
            headers: {
                "Content-Type": "application/json"
            },
            data: {
                refresh_token: refresh_token,
                partner_id: partner_id,
                merchant_id: merchant_id,
            }
        })
            .then((response) => {

                let access_token = response.data.access_token;
                let refresh_token = response.data.refresh_token;
                let expires_at_time = dateformat(new Date(new Date().setHours(new Date().getHours() + 4)), 'yyyy-mm-dd HH:MM:ss')
                let refresh_token_expires_at = dateformat(new Date(new Date().setDate(new Date().getDate() + 30)), 'yyyy-mm-dd HH:MM:ss')

                //main_account_id 별 업데이트
                execute(`UPDATE app_shopee_sync 
                SET access_token="${access_token}",
                    expires_at_time="${expires_at_time}",
                    refresh_token="${refresh_token}",
                    refresh_token_expires_at = "${refresh_token_expires_at}"
                WHERE main_account_id = ${main_account_id}`,

                    (err, rows) => {
                        if (err) {
                            throw err;
                        } else {

                            if (!bool) {
                                callback();
                                return;
                            }
                            resolve();
                            console.log(new Date() + '종료');
                            closing();
                        }
                    })

            })
            .catch((err) => {
                closing();
                console.log(err)
            })

        resolve();
    })
};

const getNewAccessToken = async () => {

    try {
        console.log(new Date() + '시작');
        await getShopeeSync();
    } catch (err) {
        console.log(err);
    }
}

getNewAccessToken();