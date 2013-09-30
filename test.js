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



var rq = new RippledQuerier(100);

// rq.getLedger(1297768);
// 1379087
// 1428745
// 1297768
// 1268267

rq.getLedgerRange(1297768, 1297769);

// rq.getLedgersByTimeRange(moment(), moment().subtract("hours", 1), function(err, ledgers){
//     if (err) {
//         winston.error(err);
//         return;
//     }

//     _.each(ledgers, function(ledger){
//         if (ledger.transactions.length > 0)
//             winston.info(JSON.stringify(ledger));
//     });

// });

// rq.searchLedgerByClosingTime(433630980);

downloadLedger(1297768, function(ledger){
    winston.info(ledger);
    winston.info(ledger.transactions.length);
});

function downloadLedger (ledger_num, callback) {

        client.get('ledgers/' + ledger_num + '.json').on('response', function(res) {

            var dataBuffer = '';

            if (404 === res.statusCode) {

                // winston.error('ledger/' + ledger_num + '.json doesn\'t exist, statusCode:', res.statusCode);
                // callback();
                // return;

            }

            if (500 === res.statusCode) {

                winston.error("got statusCode 500 for ledger:", ledger_num, ", retrying in 1 sec");
                setTimeout(function(){
                    downloadLedger(ledger_num, callback);
                }, 1000);

            }

            if (200 === res.statusCode) {

                res.on('error', function(){

                    winston.error("error while downloading ledger: " + ledger_num + " retrying in 1 sec");
                    setTimeout(function(){
                        downloadLedger(ledger_num, callback);
                    }, 1000);

                });

                res.setEncoding('utf8');

                res.on('data', function(data) {
                    dataBuffer += data;
                });

                res.on('end', function() {

                    var ledger;

                    try {

                        ledger = JSON.parse(dataBuffer);


                    } catch (e) {

                        winston.error("Malformed JSON at: ledger/" + ledger_num + ".json, trying again in 1 sec");
                        setTimeout(function(){
                            downloadLedger(ledger_num, callback);
                        }, 1000);

                    }

                    if (ledger.ledger_index !== ledger_num) {
                        callback(new Error("Ledger index for file " + ledger_num + ".json is " + ledger.ledger_index));
                        return;
                    }

                    callback(null, ledger);

                });

            }
        }).end(); // finish the client.get request

}