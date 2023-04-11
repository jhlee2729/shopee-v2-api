const axios = require('axios');
const crypto = require('crypto');
require("dotenv").config();

const getAccesstoken = () => {

    const host = 'https://partner.shopeemobile.com';
    const path = "/api/v2/auth/token/get";
    
    const code = process.env.CODE;
    const partner_id = Number(process.env.PARTNER_ID);
    const main_account_id = Number(process.env.MAIN_ACCOUNT_ID);
    const key = process.env.KEY;

    const timestamp = new Date().getTime();
    const convert = Number((timestamp.toString()).substr(0, 10));

    let stringformat = `${partner_id}${path}${convert}`;
    stringformat = stringformat.toString();

    const sign = crypto.createHmac('sha256', key).update(stringformat).digest('hex').toLowerCase();

    return axios({
            method : 'POST',
            url : `${host}${path}?partner_id=${partner_id}&timestamp=${convert}&sign=${sign}`,
            headers : {
                "Content-Type" : "application/json"
            },
            data : {
                code : code,
                partner_id : partner_id,
                main_account_id : main_account_id,
            }
        })
        .then((res)=>{
            console.log(res.data)
        })
        .catch((err)=>{
            console.log(err);
        })
}

getAccesstoken();