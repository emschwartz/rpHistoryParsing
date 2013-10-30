var winston = require('winston'),
    async = require('async'),
    _ = require('lodash');


// var couchdb = require('felix-couchdb'),
//     client = couchdb.createClient(5984, '0.0.0.0'),
//     db = client.db('rphistory'),
var config = require('./config');
var db = require('nano')('http://' + config.couchdb_username + ':' + config.couchdb_password + '@0.0.0.0:5984/rphist');

var RippledQuerier = require('./rippledquerier'),
    rq = new RippledQuerier();

var MAX_ITERATORS = 1000;
var BATCH_SIZE = 1000;

db.changes({
    limit: 20,
    descending: true
}, function(err, res) {
    if (err) {
        winston.error("Error getting last ledger saved:", err);
        return;
    }

    // find last saved ledger amongst couchdb changes stream
    var last_saved_index;

    for (var r = 0; r < res.results.length; r++) {
        if (parseInt(res.results[r].id, 10) > 0) {
            last_saved_index = parseInt(res.results[r].id, 10) - BATCH_SIZE * 10;  
            // go back further in case there was a problem and the last batch wasn't saved properly
            if (last_saved_index < 32570)
                last_saved_index = 32570;
            break;
        }
    }    

    winston.info("Starting from last saved index:", last_saved_index);

    saveNextBatch(last_saved_index + 1);
    return;
});


function saveNextBatch(batch_start) {

    rq.getLatestLedgerIndex(function(err, latest_ledger_index) {
        if (err) {
            winston.error("Error getting last ledger index:", err);
            return;
        }

        var batch_end = Math.min(latest_ledger_index, (batch_start + BATCH_SIZE));

        if (batch_start >= batch_end) {
            setTimeout(function() {
                saveNextBatch(batch_end);
            }, 10000);
            return;
        }

        var incides = _.range(batch_start, batch_end);

        rq.getLedgerRange(batch_start, batch_end, function(err, ledgers) {
            if (err) {
                winston.error("Error getting batch from", batch_start, "to", batch_end, ":", err);
                return;
            }

            var docs = [],
                ids = [];
            _.each(ledgers, function(ledger) {
                var led_num = String(ledger.ledger_index);
                var padding = "0000000000";
                var id = padding.substring(0, padding.length - led_num.length) + led_num;
                ledger._id = id;
                docs.push(ledger);
                ids.push(id);
            });

            // winston.info(JSON.stringify(docs));

            db.bulk({
                docs: ids
            }, function(err, res){
                if (err || res) {
                    console.log("err", err, "res", res);
                    return;
                }

                // db.bulk({
                //     docs: docs
                // }, function(err) {
                //     if (err) {
                //         winston.error("Error saving batch from", batch_start, "to", batch_end, ":", JSON.stringify(err));
                //         return;
                //     }

                //     if (batch_end - batch_start === 1)
                //         winston.info("Saved ledger", batch_start, "to CouchDB");
                //     else
                //         winston.info("Saved ledgers", batch_start, "to", batch_end, "to CouchDB");

                //     if (batch_end - batch_start > 1)
                //         setImmediate(function() {
                //             saveNextBatch(batch_end);
                //         });
                //     else {
                //         // winston.info("Only got", (batch_end - batch_start), "ledgers, waiting 10 sec before continuing");
                //         setTimeout(function() {
                //             saveNextBatch(batch_end);
                //         }, 10000);
                //     }
                // });
            });
        });

    });
}

function printCallback(err, result) {
    if (err) {
        winston.error(err);
    } else {
        winston.info(result);
    }
}