'use strict';

const config = require('../config');
const env = require('./env').env;
const pool = require('./connection-pool').createPool(config[env].database);
const axios = require('axios');
const crypto = require('crypto');
// const error_hook = require('./slack-lazada-order');

const syncData = {
    market : '',
    shop_id : 0,
    partner_key : '',
    partner_id : 0,
    access_token : '',
}

const contents = {
    time_range_field: '',
    time_from: 0,
    time_to: 0,
    page_size: '',
    cursor:''
}

const insertData = {
    createOrder: [],
    createOrderDetails: [],
    createMore: false,
    updateOrder: [],
    updateOrderDetails: [],
    updateMore: false,
}

const execute = (sql,callback,data = {})=>{
    
    pool.getConnection((err,connection) => {
        if (err) throw err;

        connection.query(sql,data,(err,rows) => {
            connection.release();

            if ( err ) {
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

const getAccessToken = () => {
    return new Promise((resolve,reject) => {
        execute(`SELECT access_token FROM app_shopee_sync WHERE market="${syncData.market}"`, (err,rows) => {

            if(err) throw err;
            console.log("실행할때마다 토큰값 가져오기", rows[0].access_token);
            syncData.access_token = rows[0].access_token;
            resolve();
        })
    })
}

let now = new Date();
let time = now.getTime().toString();
let time_result = Number(time.substr(0, time.length - 3));

const signature = (sign_format) => {

    sign_format = sign_format.toString();
    const sign = crypto.createHmac('sha256', syncData.partner_key).update(sign_format).digest('hex').toUpperCase();
    return sign;
}

const lastCreateTimeTo = () => {
    return new Promise((resolve,reject) => {
        execute(`SELECT time_to FROM app_shopee_api_history WHERE market="${contents.market}" 
            ORDER BY api_history_id DESC LIMIT 0,1`,
            (err,rows)=>{
                if ( err ) throw err;

                if ( rows.length >= 1 ) {
                    contents.time_from = Number(rows[0].time_to);
                    contents.time_to = time_result;
                    resolve();
                } else {
                    contents.time_from = time_result - 200000; //864000 
                    contents.time_to = time_result;
                    resolve();
                }
            });
    });
}

const createOrder = () => {
    return new Promise((resolve,reject) => {

        let path = `/api/v2/order/get_order_list`;
        let timestamp = new Date().getTime();
        let convert = Number((timestamp.toString()).substr(0, 10));
        let sign_format = `${syncData.partner_id}${path}${convert}${syncData.access_token}${syncData.shop_id}`;

        let sign = signature(sign_format);
        let cursor = 0; //offset
        let page_size = 100; //limit

        const getOrder = () => {

            axios({
                method : 'GET',
                url : "https://partner.shopeemobile.com/api/v2/order/get_order_list",
                params : {
                    partner_id : syncData.partner_id,
                    timestamp : convert,
                    access_token : syncData.access_token,
                    shop_id : syncData.shop_id,
                    sign : sign,
                    time_range_field : 'create_time',
                    time_from: contents.time_from,
                    time_to : contents.time_to,
                    page_size : page_size,
                    cursor : cursor
                }
            })
            .then((response)=>{
      
                let more = response.data.response.more;

                insertData.createOrder = insertData.createOrder.concat(response.data.response.order_list);
                insertData.createMore = more;

                if ( Boolean(more) || more == 'true' ) {
                    cursor += page_size;
                    getOrder();
                } else {
                    resolve(true);
                }
    
            })
            .catch((err)=>{
                console.log("ERR")
                console.log(err);
                resolve(false);
            })

        }
        getOrder();
    });
}

const createOrderDetails = () => {
    return new Promise((resolve,reject) => {

        let path = `/api/v2/order/get_order_detail`;
        let timestamp = new Date().getTime();
        let convert = Number((timestamp.toString()).substr(0, 10));
        let sign_format = `${syncData.partner_id}${path}${convert}${syncData.access_token}${syncData.shop_id}`;
        let sign = signature(sign_format);

        let offset = 0;
        let limit = 50;
        let sindex = 0;
        let eindex = sindex + limit;
        let order_count = insertData.createOrder.length;
        
        const getOrderDetails = (sindex,eindex) => {
            
            let orders = insertData.createOrder.slice(sindex,eindex);
            let order_sn_list = orders.map((value) => value.order_sn);
            let order_sn_list_string = order_sn_list.toString();
            
            let fields=["buyer_user_id,buyer_username,estimated_shipping_fee,recipient_address,actual_shipping_fee,goods_to_declare,note,note_update_time,item_list,pay_time,dropshipper,dropshipper_phone,split_up,buyer_cancel_reason,cancel_by,cancel_reason,actual_shipping_fee_confirmed,buyer_cpf_id,fulfillment_flag,pickup_done_time,package_list,shipping_carrier,payment_method,total_amount,invoice_data,checkout_shipping_carrier,reverse_shipping_fee,order_chargeable_weight_gram,prescription_images,prescription_check_status"];
            let fileds_string = fields.toString();
            
            // console.log(`offset: ${offset},limit:${limit}, sindex:${sindex},eindex:${eindex}, order_count:${order_count}`);
      
            axios({
                method : 'GET',
                url : "https://partner.shopeemobile.com/api/v2/order/get_order_detail",
                params : {
                    partner_id : syncData.partner_id,
                    timestamp : convert,
                    access_token : syncData.access_token,
                    shop_id : syncData.shop_id,
                    sign : sign,
                    order_sn_list : order_sn_list_string,
                    response_optional_fields : fileds_string
                }
            })
            .then((response)=>{
      
                insertData.createOrderDetails = insertData.createOrderDetails.concat(response.data.response.order_list);

                callAPI();
            })
            .catch((err)=>{
                console.log("createOrderDetails ERR");
                console.log(err);
                resolve(false);
            });
        }
        const callAPI = () => {

            offset = limit + offset;
            sindex = sindex + limit;
            eindex = eindex + limit;

            if ( order_count > offset ) {
                getOrderDetails(sindex,eindex);
            } else {
                resolve(true);
            }
        }

        getOrderDetails(sindex,eindex);
    });
}

const databaseInsert = (order,callback) => {

    let items = order.item_list;
    let count = 0;

    const check = () => {
        count++;
        count == 2 && callback();
    }

    const tomodel_order = {
        market: syncData.market,
        order_sn: order.order_sn,
        order_status: order.order_status,
        actual_shipping_fee: order.actual_shipping_fee,
        actual_shipping_fee_confirmed: order.actual_shipping_fee_confirmed,
        buyer_cancel_reason: order.buyer_cancel_reason,
        buyer_cpf_id: order.buyer_cpf_id,
        buyer_user_id: Number(order.buyer_user_id),
        buyer_username: order.buyer_username,
        cancel_by: order.cancel_by,
        cancel_reason: order.cancel_reason,
        checkout_shipping_carrier: order.checkout_shipping_carrier,
        cod : order.cod,
        create_time: order.create_time,
        update_time: order.update_time,
        currency: order.currency,
        days_to_ship: Number(order.days_to_ship),
        dropshipper: order.dropshipper,
        dropshipper_phone: order.dropshipper_phone,
        estimated_shipping_fee: Number(order.estimated_shipping_fee),
        fulfillment_flag: order.fulfillment_flag,
        goods_to_declare: order.goods_to_declare,
        invoice_data: order.invoice_data,
        message_to_seller: order.message_to_seller?.replace(/"/g, '\\"'),
        note: order.note?.replace(/"/g, '\\"'),
        note_update_time : order.note_update_time,
        order_chargeable_weight_gram: Number(order.order_chargeable_weight_gram),
        package_list_package_number: order.package_list[0].package_number,
        package_list_logistics_status: order.package_list[0].logistics_status,
        package_list_shipping_carrier: order.package_list[0].shipping_carrier,
        pay_time: Number(order.pay_time),
        payment_method: order.payment_method,
        pickup_done_time : Number(order.pickup_done_time),
        prescription_check_status: order.prescription_check_status,
        prescription_images: order.prescription_images,
        recipient_address_name: order.recipient_address?.name.replace(/"/g, '\\"'),
        recipient_address_phone: order.recipient_address.phone,
        recipient_address_town: order.recipient_address.town,
        recipient_address_district: order.recipient_address.district,
        recipient_address_city: order.recipient_address.city,
        recipient_address_state: order.recipient_address.state,
        recipient_address_region: order.recipient_address.region,
        recipient_address_zipcode: order.recipient_address.zipcode,
        recipient_address_full_address: order.recipient_address.full_address?.replace(/"/g, '\\"'),
        region: order.region,
        reverse_shipping_fee: Number(order.reverse_shipping_fee),
        ship_by_date: Number(order.ship_by_date),
        shipping_carrier: order.shipping_carrier,
        split_up: order.split_up,
        total_amount: Number(order.total_amount)
    }

    execute(`INSERT IGNORE INTO app_shopee_v2_order SET ?`,
    (err,rows)=>{
        if ( err ) {
            throw err;
        } else {
            check();
        }
    }, tomodel_order);

    // item_list
    let loop = 0;

    const loopFn = () => {

        let tomodel_items = {}

        tomodel_items.order_sn = order.order_sn;
        tomodel_items.market = syncData.market;
        
        tomodel_items.item_id = items[loop].item_id;
        tomodel_items.item_name = items[loop].item_name.replace(/"/g, '\\"');
        tomodel_items.item_sku = items[loop].item_sku;
        tomodel_items.model_id = items[loop].model_id;
        tomodel_items.model_sku = items[loop].model_sku;
        tomodel_items.model_quantity_purchased = items[loop].model_quantity_purchased;
        tomodel_items.model_original_price = items[loop].model_original_price;
        tomodel_items.model_discounted_price = items[loop].model_discounted_price;
        tomodel_items.wholesale = items[loop].wholesale;
        tomodel_items.weight = items[loop].weight;
        tomodel_items.add_on_deal = items[loop].add_on_deal;
        tomodel_items.main_item = items[loop].main_item;
        tomodel_items.add_on_deal_id = items[loop].add_on_deal_id;
        tomodel_items.promotion_type = items[loop].promotion_type;
        tomodel_items.promotion_id = items[loop].promotion_id;
        tomodel_items.image_info_image_url = items[loop].image_info.image_url;

        execute(`INSERT IGNORE INTO app_shopee_v2_order_details SET ?`,
            (err,rows)=>{
                if ( err ) {
                    throw err;
                } else {
                    (items.length == ++loop) ? check() : loopFn();
                }
            },tomodel_items);
    }

    loopFn();

}

const insertOrder = () => {
    return new Promise((resolve,reject) => {

        let loop = 0;
        const callAPI = () => {

            insertData.createOrderDetails.length == loop ? 
                resolve() :
                databaseInsert(insertData.createOrderDetails[loop++],callAPI);
        }
        databaseInsert(insertData.createOrderDetails[loop++],callAPI);
    });
}

const connectionClose = (callback,bool) => {
    return new Promise((resolve,reject) => {

        console.log(insertData.createOrder.length);
        console.log(insertData.createOrderDetails.length);
        console.log(insertData.updateOrder.length);
        console.log(insertData.updateOrderDetails.length);
        console.log(new Date() + ' 종료');
        console.log('=====================================================================');
        console.log(bool);
        if ( bool ) {
            closing();
        }
        callback();
    });
}

const worker = async(sync,callback,bool) => {

    try {
        
        console.log('=====================================================================');
        console.log(new Date() + ' 시작');

        // insertData 초기화
        insertData.createOrder = [];
        insertData.createOrderDetails = [];
        insertData.createMore = false;
        insertData.updateOrder = [];
        insertData.updateOrderDetails = [];
        insertData.updateMore = false;

        syncData.market = sync.market;
        syncData.shop_id = sync.shop_id;
        syncData.partner_key = sync.partner_key;
        syncData.partner_id = sync.partner_id;
        syncData.access_token = sync.access_token;

        await lastCreateTimeTo();
        const success1 = await createOrder();
        let success_details_1 = true;

        if ( insertData.createOrder.length != 0 ) {
            success_details_1 = await createOrderDetails();
        }

        if ( !success1 ) {
            await connectionClose(callback,bool);
            return;
        }

        insertData.createOrder.length !=0 && await insertOrder();

    } catch(e){
        console.log(e);
    }
}

module.exports = worker;