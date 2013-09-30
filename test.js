var winston = require('winston'),
    ripple = require('ripple-lib'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    knox = require('knox'),
    diff = require('deep-diff'),
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

// rq.getLedgerRange(1297768, 1297769);

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


compareS3toDB(1268267);



function compareS3toDB(ledger_index) {

    downloadLedger(ledger_index, function(err, dwn_ledger) {
        if (err) {
            winston.error(err);
            return;
        }
        rq.getLedger(ledger_index, function(err, db_ledger) {
            var differences = diff(dwn_ledger, db_ledger);
            winston.info(JSON.stringify(differences));
        });
        // winston.info(ledger.transactions.length);
    });

}

function downloadLedger(ledger_num, callback) {

    client.get('ledgers/' + ledger_num + '.json').on('response', function(res) {

        var dataBuffer = '';

        if (200 === res.statusCode) {

            res.on('error', function() {

                winston.error("error while downloading ledger: " + ledger_num + " retrying in 1 sec");
                setTimeout(function() {
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
                    callback(null, ledger);


                } catch (e) {

                    callback(e);

                }

                // if (ledger.ledger_index !== ledger_num) {
                //     callback(new Error("Ledger index for file " + ledger_num + ".json is " + ledger.ledger_index));
                //     return;
                // }



            });

        }
    }).end(); // finish the client.get request

}