const crypto = require('crypto');
require("dotenv").config();

const signature = () => {

    const host = 'https://partner.shopeemobile.com';
    const path = '/api/v2/shop/auth_partner';
    const redirect = 'https://www.daum.net/';

    const partner_id = Number(process.env.PARTNER_ID);
    const key = process.env.KEY;

    const timestamp = new Date().getTime();
    const convert = Number((timestamp.toString()).substr(0, 10));
    let stringformat = `${partner_id}${path}${convert}`;

    stringformat = stringformat.toString();

    const hash = crypto.createHmac('sha256', key).update(stringformat).digest('hex');

    const url = `${host}${path}?partner_id=${partner_id}&timestamp=${convert}&sign=${hash}&redirect=${redirect}`;
    return url;

}

console.log(signature());  // 로그인 인증(권한부여 방식)에 따라 code, shop_id or main_account_id 값을 받음