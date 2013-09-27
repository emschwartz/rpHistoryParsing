var winston = require('winston'),
    ripple = require('ripple-lib'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    knox = require('knox'),
    RippledQuerier = require('./rippledquerier').RippledQuerier;

var config = require('./config');

var client = knox.createClient({
    key: config.s3.key,
    secret: config.s3.secret,
    bucket: config.s3.bucket
});

var MAX_UPLOADERS = 25;

var start_epoch = 410227200; // rpepoch corresponding to "2012-12-31-T00:00:00+00:00"

// var rq = new RippledQuerier(1000);

var first_timestamp = ripple.utils.toTimestamp(410325670);
var first_moment = moment(first_timestamp);
winston.info(first_moment.format());

var first_day_start = moment("2012-12-31-T00:00:00+00:00").valueOf();
winston.info(first_day_start);

var start_epoch = rpEpochFromTimestamp(1356912000000);
winston.info(start_epoch);

function rpEpochFromTimestamp(timestamp) {
    return timestamp / 1000 - 0x386D4380;
}