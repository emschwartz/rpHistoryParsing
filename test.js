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



var rq = new RippledQuerier(10);

// rq.getLedger(2439819);


rq.getLedgersByTimeRange(moment(), moment().subtract("hours", 1), function(err, ledgers){
    if (err) {
        winston.error(err);
        return;
    }

    winston.info("Got this many ledgers:", ledgers.length);

});

// rq.searchLedgerByClosingTime(433630980);

