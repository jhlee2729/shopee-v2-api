const Slack = require('slack-node');
const webhookUri = "https://hooks.slack.com/services/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";
const slack = new Slack();

slack.setWebhook(webhookUri);

const send = async(market, message, callback) => {

    slack.webhook({
        channel: "#error-shopee-order", // 전송될 슬랙 채널
        username: "shopee-v2-api", //슬랙에 표시될 이름
        text: market + ' - ' + JSON.stringify(message)
    }, callback);
}

module.exports = send;