'use strict';
/* Shopee Order API : V2.0 UPDATE - V1.0 TABLE column matching Version */

const config = require('../config');
const env = require('./env').env;
const pool = require('./connection-pool').createPool(config[env].database);
const axios = require('axios');
const crypto = require('crypto');
const error_hook = require('./slackhook');

const syncData = {
    market : '',
    shop_id : 0,
    partner_key : '',
    partner_id : 0,
    access_token : '',
}

const contents = {
    time_from: 0,
    time_to: 0,
}

const insertData = {
    createOrder: [],
    createOrderDetails: [],
    createMore: false,
    updateOrder: [],
    updateOrderDetails: [],
    updateMore: false,
    updateShippingDocument: []
}

const execute = (sql,callback,data = {}) => {
    
    pool.getConnection((err,connection) => {
        if (err) throw err;

        connection.query(sql,data,(err,rows) => {
            connection.release();

            if ( err ) {
                error_hook(syncData.market,err,(e,res) => {
                    console.log("execute", err);
                    throw err;
                });
            } else {
                callback(err, rows);
            }
        });
    });
}

const remove_emoji = function(text){
        
    return text.replace(/[\{\}\[\]\/?.,;:|\)*~`!^\-_+<>@\$&\\\=\(\'\"]|[\u2700-\u27BF]|[\uE000-\uF8FF]|\uD83C[\uDC00-\uDFFF]|\uD83D[\uDC00-\uDFFF]|[\u2011-\u26FF]|\uD83E[\uDD10-\uDDFF]/gi, '');
}

const closing = () => {
    pool.end();
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
        execute(`SELECT time_to FROM app_shopee_api_history WHERE market="${syncData.market}" 
            ORDER BY api_history_id DESC LIMIT 0,1`,
            (err,rows) => {

                if ( err ) throw err;
                if ( rows.length >= 1 ) {
                    contents.time_from = Number(rows[0].time_to);
                    contents.time_to = time_result;
                    resolve();
                } else {
                    contents.time_from = time_result - 86400;
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
            }).then((response) => {
      
                let more = response.data.response.more;

                insertData.createOrder = insertData.createOrder.concat(response.data.response.order_list);
                insertData.createMore = more;

                if ( Boolean(more) || more == 'true' ) {
                    cursor += page_size;
                    getOrder();
                } else {
                    resolve(true);
                }
    
            }).catch((err) => {
                error_hook(syncData.market,err,(e,res) => {
                    console.log("createOrder 에러", err);
                    resolve(false);
                });
            })

        }
        getOrder();
    });
}

const createOrderDetailsTake = () => {
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
            }).then((response) => {
      
                insertData.createOrderDetails = insertData.createOrderDetails.concat(response.data.response.order_list);
                callAPI();

            }).catch((err) => {
                error_hook(syncData.market,err,(e,res) => {
                    console.log("createOrderDetailsTake 에러", err);
                    resolve(false);
                });
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

const updateOrder = () => {
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
                    time_range_field : 'update_time',
                    time_from: contents.time_from - 1200, // 20분전 (업데이트 주문)
                    time_to : contents.time_to,
                    page_size : page_size,
                    cursor : cursor
                }
            }).then((response) => {

                let more = response.data.response.more;

                insertData.updateOrder = insertData.updateOrder.concat(response.data.response.order_list);
                insertData.updateMore = more;

                if ( Boolean(more) || more == 'true' ) {
                    cursor += page_size;
                    getOrder();
                } else {
                    resolve(true);
                }
    
            }).catch((err) => {
                error_hook(syncData.market,err,(e,res) => {
                    console.log("updateOrder 에러", err);
                    resolve(false);
                });
            })

        }
        getOrder();
    });
}

const updateOrderDetailsTake = () => {
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
        let order_count = insertData.updateOrder.length;
        
        const getOrderDetails = (sindex,eindex) => {
            
            let orders = insertData.updateOrder.slice(sindex,eindex);
            let order_sn_list = orders.map((value) => value.order_sn);
            let order_sn_list_string = order_sn_list.toString();
            
            let fields=["buyer_user_id,buyer_username,estimated_shipping_fee,recipient_address,actual_shipping_fee,goods_to_declare,note,note_update_time,item_list,pay_time,dropshipper,dropshipper_phone,split_up,buyer_cancel_reason,cancel_by,cancel_reason,actual_shipping_fee_confirmed,buyer_cpf_id,fulfillment_flag,pickup_done_time,package_list,shipping_carrier,payment_method,total_amount,invoice_data,checkout_shipping_carrier,reverse_shipping_fee,order_chargeable_weight_gram,prescription_images,prescription_check_status"];
            let fileds_string = fields.toString();
            
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
            }).then((response) => {

                insertData.updateOrderDetails = insertData.updateOrderDetails.concat(response.data.response.order_list);
                callAPI();

            }).catch((err) => {
                error_hook(syncData.market,err,(e,res) => {
                    console.log("updateOrderDetailsTake 에러", err);
                    resolve(false);
                });
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

const getShipDocumentInfo = () => {
    return new Promise((resolve,reject) => {
        
        let path = `/api/v2/logistics/get_shipping_document_data_info`
        let timestamp = new Date().getTime();
        let convert = Number((timestamp.toString()).substr(0, 10));
        let sign_format = `${syncData.partner_id}${path}${convert}${syncData.access_token}${syncData.shop_id}`;
        let sign = signature(sign_format);

        let order_count = insertData.updateOrder.length;
        let loop = 0;

        const getShipDocument = () => {

            axios({
                method : 'POST',
                url : "https://partner.shopeemobile.com/api/v2/logistics/get_shipping_document_data_info",
                params : {
                    partner_id : syncData.partner_id,
                    timestamp : convert,
                    access_token : syncData.access_token,
                    shop_id : syncData.shop_id,
                    sign : sign,
                    order_sn : insertData.updateOrder[loop].order_sn,
                    recipient_address_info: [{ "key": "name" }]
                }
            }).then((response) => {
                
                if ( response.data.response !== undefined) {
                    insertData.updateShippingDocument = insertData.updateShippingDocument.concat({'order_sn': insertData.updateOrder[loop].order_sn, 'tracking_number': response.data.response.shipping_document_info.tracking_number, 'service_code': response.data.response.shipping_document_info.service_code})
                }
                loop++;
                callAPI();

            }).catch((err) => {
                error_hook(syncData.market,err,(e,res) => {
                    console.log("getShipDocumentInfo 에러", err);
                    resolve(false);
                });
            })

        }

        const callAPI = () => {
            if ( order_count != loop ) {
                getShipDocument();
            } else {
                console.log(`총수량 : ${insertData.updateOrder.length}, 업데이트수량 : ${insertData.updateShippingDocument.length}`)
                resolve(insertData.updateShippingDocument);
            }
        }

        getShipDocument();
    })
}

const createOrderDetailsBundle = () => {
    return new Promise((resolve,reject) => {

        if ( insertData.createOrderDetails.length > 0 ) {

            let loop = 0;

            const func = () => {

                let items = insertData.createOrderDetails[loop].item_list;
                let bundle = [];

                for ( let i in items ) {
                    let check = true;
                    
                    for ( let k in bundle ) {

                        if ( items[i].item_id == bundle[k].item_id 
                            && items[i].model_id == bundle[k].model_id
                            && items[i].add_on_deal_id == bundle[k].add_on_deal_id
                            && items[i].promotion_id == bundle[k].promotion_id ) {
                            bundle[k].model_quantity_purchased = Number(bundle[k].model_quantity_purchased) + Number(items[i].model_quantity_purchased);

                            check = false;
                        }
                    }
                
                    if ( check ) {
                        bundle.push(items[i]);
                    }
                }

                insertData.createOrderDetails[loop].item_list = bundle;
                (++loop==insertData.createOrderDetails.length) ? resolve() : func();
            }
            func();
        } else {
            resolve();
        }
    });
}

const updateOrderDetailsBundle = () => {
    return new Promise((resolve,reject) => {

        if ( insertData.updateOrderDetails.length > 0 ) {

            let loop = 0;

            const func = () => {

                let items = insertData.updateOrderDetails[loop].item_list;
                let bundle = [];

                for ( let i in items ) {

                    let check = true;
                    
                    for ( let k in bundle ) {

                        if ( items[i].item_id == bundle[k].item_id 
                            && items[i].model_id == bundle[k].model_id
                            && items[i].add_on_deal_id == bundle[k].add_on_deal_id
                            && items[i].promotion_id == bundle[k].promotion_id ) {
                            bundle[k].model_quantity_purchased = Number(bundle[k].model_quantity_purchased) + Number(items[i].model_quantity_purchased);
                            check = false;
                        }
                    }
                
                    if ( check ) {
                        bundle.push(items[i]);
                    }
                }

                insertData.updateOrderDetails[loop].item_list = bundle;
                (++loop==insertData.updateOrderDetails.length) ? resolve() : func();
            }
            func();
        } else {
            resolve();
        }
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
        ordersn: order.order_sn,
        country: order.region,
        currency: order.currency,
        cod: (order.cod === true) ? 1 : 0,
        days_to_ship: Number(order.days_to_ship),
        name: order.recipient_address?.name.replace(/"/g, '\\"'),
        phone: order.recipient_address.phone,
        town: order.recipient_address.town,
        district: order.recipient_address.district,
        city: order.recipient_address.city,
        state: order.recipient_address.state,
        recipient_country: order.recipient_address.region,
        zipcode: order.recipient_address.zipcode,
        full_address: order.recipient_address.full_address?.replace(/"/g, '\\"'),
        estimated_shipping_fee: Number(order.estimated_shipping_fee),
        actual_shipping_cost: order.actual_shipping_fee,
        total_amount: Number(order.total_amount),
        order_status: order.order_status,
        shipping_carrier: order.shipping_carrier,
        payment_method: order.payment_method,
        goods_to_declare: (order.goods_to_declare === true) ? 1 : 0,
        message_to_seller: remove_emoji(order.message_to_seller)?.replace(/"/g, '\\"'),
        note: order.note?.replace(/"/g, '\\"'),
        note_update_time : order.note_update_time,
        create_time: order.create_time,
        update_time: order.update_time,
        pay_time: Number(order.pay_time),
        dropshipper: order.dropshipper,
        buyer_username: order.buyer_username,
        dropshipper_phone: order.dropshipper_phone,
        ship_by_date: Number(order.ship_by_date),
        is_split_up: (order.split_up === true) ? 1 : 0,
        buyer_cancel_reason: order.buyer_cancel_reason,
        cancel_by: order.cancel_by,
        cancel_reason: order.cancel_reason,
        is_actual_shipping_fee_confirmed: (order.actual_shipping_fee_confirmed === true) ? 1 : 0,
        buyer_cpf_id: order.buyer_cpf_id,
        buyer_user_id: Number(order.buyer_user_id),
        checkout_shipping_carrier: order.checkout_shipping_carrier,
        fulfillment_flag: order.fulfillment_flag,
        invoice_data: order.invoice_data,
        order_chargeable_weight_gram: Number(order.order_chargeable_weight_gram),
        package_list_package_number: order.package_list[0].package_number,
        package_list_logistics_status: order.package_list[0].logistics_status,
        package_list_shipping_carrier: order.package_list[0].shipping_carrier,
        pickup_done_time : Number(order.pickup_done_time),
        prescription_check_status: order.prescription_check_status,
        prescription_images: order.prescription_images,
        reverse_shipping_fee: Number(order.reverse_shipping_fee),
    }

    execute(`INSERT IGNORE INTO app_shopee_order SET ?`,
        (err,rows) => {
            if ( err ) {
                error_hook(syncData.market,err,(e,res) => {
                    console.log("databaseInsert app_shopee_order 에러", err);
                    throw err;
                });
            } else {
                check();
            }
        }, tomodel_order);

    // item_list
    let loop = 0;

    const loopFn = () => {

        let tomodel_items = {};
        tomodel_items.ordersn = order.order_sn;
        tomodel_items.item_id = items[loop].item_id;
        tomodel_items.item_name = items[loop].item_name.replace(/"/g, '\\"');
        tomodel_items.item_sku = items[loop].item_sku;
        tomodel_items.variation_id = items[loop].model_id;
        tomodel_items.variation_name = items[loop].model_name;
        tomodel_items.variation_sku = items[loop].model_sku;
        tomodel_items.variation_quantity_purchased = items[loop].model_quantity_purchased;
        tomodel_items.variation_original_price = Number(items[loop].model_original_price);
        tomodel_items.variation_discounted_price = items[loop].model_discounted_price === undefined ? 0 : Number(items[loop].model_discounted_price);
        tomodel_items.is_wholesale = (items[loop].wholesale === true) ? 1 : 0;
        tomodel_items.weight = Number(items[loop].weight);
        tomodel_items.is_add_on_deal = items[loop].add_on_deal;
        tomodel_items.is_main_item = (items[loop].main_item === true) ? 1 : 0;
        tomodel_items.add_on_deal_id = items[loop].add_on_deal_id;
        tomodel_items.promotion_type = items[loop].promotion_type;
        tomodel_items.promotion_id = items[loop].promotion_id;
        tomodel_items.image_info_image_url = items[loop].image_info.image_url;

        execute(`INSERT IGNORE INTO app_shopee_order_details SET ?`,
            (err,rows) => {
                if ( err ) {
                    error_hook(syncData.market,err,(e,res) => {
                        console.log("databaseInsert app_shopee_order_details 에러", err);
                        throw err;
                    });
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

const databaseReplace = (order,callback) => {

    let items = order.item_list;
    let count = 0;

    const check = () => {
        count++;
        count == 2 && callback();
    }

    //order
    execute(`INSERT INTO app_shopee_order
        (
            market,
            ordersn,
            country,
            currency,
            cod,
            days_to_ship,
            name,
            phone,
            town,
            district,
            city,
            state,
            recipient_country,
            zipcode,
            full_address,
            estimated_shipping_fee,
            actual_shipping_cost,
            total_amount,
            order_status,
            shipping_carrier,
            payment_method,
            goods_to_declare,
            message_to_seller,
            note,
            note_update_time ,
            create_time,
            update_time,
            pay_time,
            dropshipper,
            buyer_username,
            dropshipper_phone,
            ship_by_date,
            is_split_up,
            buyer_cancel_reason,
            cancel_by,
            cancel_reason,
            is_actual_shipping_fee_confirmed,
            buyer_cpf_id,
            buyer_user_id,
            checkout_shipping_carrier,
            fulfillment_flag,
            invoice_data,
            order_chargeable_weight_gram,
            package_list_package_number,
            package_list_logistics_status,
            package_list_shipping_carrier,
            pickup_done_time ,
            prescription_check_status,
            prescription_images,
            reverse_shipping_fee
        )
        VALUES
        (
            "${syncData.market}",
            "${order.order_sn}",
            "${order.region}",
            "${order.currency}",
            "${(order.cod === true) ? 1 : 0}",
            ${Number(order.days_to_ship)},
            "${order.recipient_address?.name.replace(/"/g, '\\"')}",
            "${order.recipient_address.phone}",
            "${order.recipient_address.town}",
            "${order.recipient_address.district}",
            "${order.recipient_address.city}",
            "${order.recipient_address.state}",
            "${order.recipient_address.region}",
            "${order.recipient_address.zipcode}",
            "${order.recipient_address.full_address?.replace(/"/g, '\\"')}",
            ${Number(order.estimated_shipping_fee)},
            "${order.actual_shipping_fee}",
            ${Number(order.total_amount)},
            "${order.order_status}",
            "${order.shipping_carrier}",
            "${order.payment_method}",
            "${(order.goods_to_declare === true) ? 1 : 0}",
            "${remove_emoji(order.message_to_seller)?.replace(/"/g, '\\"')}",
            "${order.note?.replace(/"/g, '\\"')}",
            ${order.note_update_time},
            ${order.create_time},
            ${order.update_time},
            ${Number(order.pay_time)},
            "${order.dropshipper}",
            "${order.buyer_username}",
            "${order.dropshipper_phone}",
            ${Number(order.ship_by_date)},
            "${(order.split_up === true) ? 1 : 0}",
            "${order.buyer_cancel_reason}",
            "${order.cancel_by}",
            "${order.cancel_reason}",
            "${(order.actual_shipping_fee_confirmed === true) ? 1 : 0}",
            "${order.buyer_cpf_id}",
            ${Number(order.buyer_user_id)},
            "${order.checkout_shipping_carrier}",
            "${order.fulfillment_flag}",
            "${order.invoice_data}",
            ${Number(order.order_chargeable_weight_gram)},
            "${order.package_list[0].package_number}",
            "${order.package_list[0].logistics_status}",
            "${order.package_list[0].shipping_carrier}",
            ${Number(order.pickup_done_time)},
            "${order.prescription_check_status}",
            "${order.prescription_images}",
            ${Number(order.reverse_shipping_fee)}
        )
        ON DUPLICATE KEY UPDATE
            market = "${syncData.market}",
            ordersn = "${order.order_sn}",
            country = "${order.region}",
            currency = "${order.currency}",
            cod = "${(order.cod === true) ? 1 : 0}",
            days_to_ship = ${Number(order.days_to_ship)},
            name = "${order.recipient_address?.name.replace(/"/g, '\\"')}",
            phone = "${order.recipient_address.phone}",
            town = "${order.recipient_address.town}",
            district = "${order.recipient_address.district}",
            city = "${order.recipient_address.city}",
            state = "${order.recipient_address.state}",
            recipient_country = "${order.recipient_address.region}",
            zipcode = "${order.recipient_address.zipcode}",
            full_address = "${order.recipient_address.full_address?.replace(/"/g, '\\"')}",
            estimated_shipping_fee = ${Number(order.estimated_shipping_fee)},
            actual_shipping_cost = "${order.actual_shipping_fee}",
            total_amount = ${Number(order.total_amount)},
            order_status = "${order.order_status}",
            shipping_carrier = "${order.shipping_carrier}",
            payment_method = "${order.payment_method}",
            goods_to_declare = "${(order.goods_to_declare === true) ? 1 : 0}",
            message_to_seller = "${remove_emoji(order.message_to_seller)?.replace(/"/g, '\\"')}",
            note = "${order.note?.replace(/"/g, '\\"')}",
            note_update_time = ${ order.note_update_time},
            create_time = ${order.create_time},
            update_time = ${order.update_time},
            pay_time = ${Number(order.pay_time)},
            dropshipper = "${order.dropshipper}",
            buyer_username = "${order.buyer_username}",
            dropshipper_phone = "${order.dropshipper_phone}",
            ship_by_date = ${Number(order.ship_by_date)},
            is_split_up = "${(order.split_up === true) ? 1 : 0}",
            buyer_cancel_reason = "${order.buyer_cancel_reason}",
            cancel_by = "${order.cancel_by}",
            cancel_reason = "${order.cancel_reason}",
            is_actual_shipping_fee_confirmed = "${(order.actual_shipping_fee_confirmed === true) ? 1 : 0}",
            buyer_cpf_id = "${order.buyer_cpf_id}",
            buyer_user_id = ${Number(order.buyer_user_id)},
            checkout_shipping_carrier = "${order.checkout_shipping_carrier}",
            fulfillment_flag = "${order.fulfillment_flag}",
            invoice_data = "${order.invoice_data}",
            order_chargeable_weight_gram = ${Number(order.order_chargeable_weight_gram)},
            package_list_package_number = "${order.package_list[0].package_number}",
            package_list_logistics_status = "${order.package_list[0].logistics_status}",
            package_list_shipping_carrier = "${order.package_list[0].shipping_carrier}",
            pickup_done_time = ${Number(order.pickup_done_time)},
            prescription_check_status = "${order.prescription_check_status}",
            prescription_images = "${order.prescription_images}",
            reverse_shipping_fee = ${Number(order.reverse_shipping_fee)}
        `,
        (err,rows) => {
            if ( err ) {
                error_hook(syncData.market,err,(e,res) => {
                    console.log("databaseReplace app_shopee_order 에러", err);
                    throw err;
                });
            } else {
                check();
            }
        }, {});
            
    // items
    let loop = 0;

    const loopFn = () => {
        execute(`INSERT INTO app_shopee_order_details 
            (
                ordersn,
                item_id,
                item_name,
                item_sku,
                variation_id,
                variation_name,
                variation_sku,
                variation_quantity_purchased,
                variation_original_price,
                variation_discounted_price,
                is_wholesale,
                weight,
                is_add_on_deal,
                is_main_item,
                add_on_deal_id,
                promotion_type,
                promotion_id,
                image_info_image_url
            )
            VALUES
            (
                "${order.order_sn}",
                ${items[loop].item_id},
                "${items[loop].item_name.replace(/"/g, '\\"')}",
                "${items[loop].item_sku}",
                ${items[loop].model_id},
                "${items[loop].model_name}",
                "${items[loop].model_sku}",
                "${items[loop].model_quantity_purchased}",
                ${Number(items[loop].model_original_price)},
                ${items[loop].model_discounted_price === undefined ? 0 : Number(items[loop].model_discounted_price)},
                "${(items[loop].wholesale === true) ? 1 : 0}",
                ${Number(items[loop].weight)},
                ${items[loop].add_on_deal},
                "${(items[loop].main_item === true) ? 1 : 0}",
                ${items[loop].add_on_deal_id},
                "${items[loop].promotion_type}",
                ${items[loop].promotion_id},
                "${items[loop].image_info.image_url}"
            )
            ON DUPLICATE KEY UPDATE
                ordersn = "${order.order_sn}",
                item_id = ${items[loop].item_id},
                item_name = "${items[loop].item_name.replace(/"/g, '\\"')}",
                item_sku = "${items[loop].item_sku}",
                variation_id = ${items[loop].model_id},
                variation_name = "${items[loop].model_name}",
                variation_sku = "${items[loop].model_sku}",
                variation_quantity_purchased = "${items[loop].model_quantity_purchased}",
                variation_original_price = ${Number(items[loop].model_original_price)},
                variation_discounted_price = ${items[loop].model_discounted_price === undefined ? 0 : Number(items[loop].model_discounted_price)},
                is_wholesale = "${(items[loop].wholesale === true) ? 1 : 0}",
                weight = ${Number(items[loop].weight)},
                is_add_on_deal = ${items[loop].add_on_deal},
                is_main_item = "${(items[loop].main_item === true) ? 1 : 0}",
                add_on_deal_id = ${items[loop].add_on_deal_id},
                promotion_type = "${items[loop].promotion_type}",
                promotion_id = ${items[loop].promotion_id},
                image_info_image_url = "${items[loop].image_info.image_url}"
            `,
            (err,rows) => {
                if ( err ) {
                    error_hook(syncData.market,err,(e,res) => {
                        console.log("databaseReplace app_shopee_order_details 에러", err);
                        throw err;
                    });
                } else {
                    (items.length == ++loop) ? check() : loopFn();
                }
            }, {});
    }
    loopFn();
}

const editOrder = () => {
    return new Promise((resolve,reject) => {

        let loop = 0;
        const callAPI = () => {

            insertData.updateOrderDetails.length == loop ? 
                resolve() :
                databaseReplace(insertData.updateOrderDetails[loop++],callAPI);
        }
        databaseReplace(insertData.updateOrderDetails[loop++],callAPI);
    });
}

const databaseUpdateTracking = (trackingData) => {
    return new Promise((resolve,reject) => {
        
        trackingData.forEach(i => { 
            console.log("updateData", i.order_sn, i.tracking_number, i.service_code);
            execute(`UPDATE app_shopee_order
                        SET tracking_no="${i.tracking_number}", service_code ="${i.service_code}"
                        WHERE ordersn = "${i.order_sn}";`,
                (err,rows) => {
                    if (err) {
                        error_hook(syncData.market,err,(e,res) => {
                            console.log("databaseUpdateTracking 에러", err);
                            throw err;
                        });
                    }
                }, {});
        })
        resolve();
    });
}

const timeSave = () => {
    return new Promise((resolve,reject) => {
        
        execute(`INSERT INTO app_shopee_api_history
            (
                market,
                time_to,
                create_count,
                update_count,
                create_more,
                update_more
            ) VALUES (
                "${syncData.market}",
                ${contents.time_to + 1},
                ${insertData.createOrder.length},
                ${insertData.updateOrder.length},
                "${insertData.createMore}",
                "${insertData.updateMore}"
            )`,
            (err,rows) => {
                if ( err ) {
                    throw err;
                } else {
                    resolve();
                }
            }, {});
    });
}

const countSave = () => {
    return new Promise((resolve,reject) => {
        
        execute(`INSERT INTO app_shopee_count
            (
                market,
                TOTAL,
                UNPAID,
                READY_TO_SHIP,
                PROCESSED,
                RETRY_SHIP,
                SHIPPED,
                TO_CONFIRM_RECEIVE,
                IN_CANCEL,
                CANCELLED,
                TO_RETURN,
                COMPLETED
            ) VALUES (
                "${syncData.market}",
                (SELECT COUNT(*) count FROM app_shopee_order WHERE market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="UNPAID" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="READY_TO_SHIP" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="PROCESSED" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="RETRY_SHIP" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="SHIPPED" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="TO_CONFIRM_RECEIVE" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="IN_CANCEL" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="CANCELLED" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="TO_RETURN" AND market="${syncData.market}"),
                (SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="COMPLETED" AND market="${syncData.market}")
            ) ON DUPLICATE KEY UPDATE
                TOTAL=(SELECT COUNT(*) count FROM app_shopee_order WHERE market="${syncData.market}"),
                UNPAID=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="UNPAID" AND market="${syncData.market}"),
                READY_TO_SHIP=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="READY_TO_SHIP" AND market="${syncData.market}"),
                PROCESSED =(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="PROCESSED" AND market="${syncData.market}"),
                RETRY_SHIP=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="RETRY_SHIP" AND market="${syncData.market}"),
                SHIPPED=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="SHIPPED" AND market="${syncData.market}"),
                TO_CONFIRM_RECEIVE=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="TO_CONFIRM_RECEIVE" AND market="${syncData.market}"),
                IN_CANCEL=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="IN_CANCEL" AND market="${syncData.market}"),
                CANCELLED=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="CANCELLED" AND market="${syncData.market}"),
                TO_RETURN=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="TO_RETURN" AND market="${syncData.market}"),
                COMPLETED=(SELECT COUNT(*) count FROM app_shopee_order WHERE order_status="COMPLETED" AND market="${syncData.market}");`,
            (err,rows) => {
                if ( err ) {
                    error_hook(syncData.market,err,(e,res) => {
                        console.log("countSave 에러", err);
                        throw err;
                    });
                } else {
                    resolve();
                }
            }, {});
    });
}

const connectionClose = (callback,bool) => {
    return new Promise((resolve,reject) => {

        console.log(syncData.market, insertData.createOrder.length, insertData.createOrderDetails.length, insertData.updateOrder.length, insertData.updateOrderDetails.length);
        console.log(new Date() + ' 종료');
        console.log('=====================================================================');

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

        insertData.createOrder = [];
        insertData.createOrderDetails = [];
        insertData.createMore = false;
        insertData.updateOrder = [];
        insertData.updateOrderDetails = [];
        insertData.updateMore = false;
        insertData.updateShippingDocument = [];

        syncData.market = sync.market;
        syncData.shop_id = sync.shop_id;
        syncData.partner_key = sync.partner_key;
        syncData.partner_id = sync.partner_id;
        syncData.access_token = sync.access_token;

        await lastCreateTimeTo();
        const success1 = await createOrder();
        let success_details_1 = true;

        if ( insertData.createOrder.length != 0 ) {
            success_details_1 = await createOrderDetailsTake();
        }

        const success2 = await updateOrder();
        let success_details_2 = true;
        
        if ( insertData.updateOrder.length != 0 ) { 
            success_details_2 = await updateOrderDetailsTake();
        }

        if ( !success1 ) {
            await connectionClose(callback,bool);
            return;
        }
        
        if ( !success_details_1 ) {
            await connectionClose(callback,bool);
            return;
        }

        if ( !success2 ) {
            await connectionClose(callback,bool);
            return;
        }

        if ( !success_details_2 ) {
            await connectionClose(callback,bool);
            return;
        }

        let c_count = insertData.createOrder.length;
        let cd_count = insertData.createOrderDetails.length;
        let u_count = insertData.updateOrder.length;
        let ud_count = insertData.updateOrderDetails.length;

        await createOrderDetailsBundle();
        await updateOrderDetailsBundle();

        if ( c_count == cd_count && u_count == ud_count ) {
            c_count != 0 && await insertOrder();
            u_count != 0 && await editOrder();
        }
        
        // 트래킹정보 업데이트
        if (u_count != 0 ) {
            let trackingData = await getShipDocumentInfo();
            if ( trackingData !== false ) {
                await databaseUpdateTracking(trackingData);
            }
        }

        await timeSave();
        await countSave();
        await connectionClose(callback,bool);

    } catch(e){
        console.log(e);
    }
}

module.exports = worker;

