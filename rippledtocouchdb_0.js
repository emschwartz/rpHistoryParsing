var winston = require('winston'),
    async = require('async'),
    _ = require('lodash');


// var couchdb = require('felix-couchdb'),
//     client = couchdb.createClient(5984, '0.0.0.0'),
//     db = client.db('rphistory'),
var config = require('./config');
var db = require('nano')('http://' + config.couchdb_username + ':' + config.couchdb_password + '@0.0.0.0:5984/rphistory');

var RippledQuerier = require('./rippledquerier'),
    rq = new RippledQuerier();

var MAX_ITERATORS = 1000;
var BATCH_SIZE = 1;


// if (process.argv.length === 3) {
//     saveNextBatch(parseInt(process.argv[2]), 10);
// }



// db.changes({
//     limit: 20,
//     descending: true
// }, function(err, res) {
//     if (err) {
//         winston.error("Error getting last ledger saved:", err);
//         return;
//     }

//     // find last saved ledger amongst couchdb changes stream
//     var last_saved_index;
//     for (var r = 0; r < res.results.length; r++) {
//         try {
//             last_saved_index = JSON.parse(res.results[r].id, 10);
//             break;
//         } catch (e) {
//             continue;
//         }
//     }

//     winston.info("Starting from last saved index:", last_saved_index);

//     saveNextBatch(last_saved_index + 1);
//     return;
// });

rq.getLedger(0);

saveNextBatch();


function saveNextBatch() {

    // rq.getLatestLedgerIndex(function(err, latest_ledger_index) {
    //     if (err) {
    //         winston.error("Error getting last ledger index:", err);
    //         return;
    //     }

    //     var batch_end = Math.min(latest_ledger_index, (batch_start + BATCH_SIZE));

        // if (batch_start === batch_end) {
        //     setTimeout(function() {
        //         saveNextBatch(batch_end);
        //     }, 10000);
        //     return;
        // }

        // var batch_end = 1;


        rq.getLedger(0, function(err, ledger) {
            if (err) {
                winston.error("Error getting batch from", batch_start, "to", batch_end, ":", err);
                return;
            }

            var ledgers = [];
            ledgers.push(ledger);

            var docs = _.map(ledgers, function(ledger) {
                ledger._id = String(ledger.ledger_index);
                return ledger;
            });

            // winston.info(JSON.stringify(docs));

            db.bulk({
                docs: docs
            }, function(err) {
                if (err) {
                    winston.error("Error saving batch from", batch_start, "to", batch_end, ":", JSON.stringify(err));
                    return;
                }

                if (batch_end - batch_start === 1)
                    winston.info("Saved ledger", batch_start, "to CouchDB");
                else
                    winston.info("Saved ledgers", batch_start, "to", batch_end, "to CouchDB");


                if (batch_end - batch_start > 1)
                    setImmediate(function() {
                        saveNextBatch(batch_end);
                    });
                else {
                    // winston.info("Only got", (batch_end - batch_start), "ledgers, waiting 10 sec before continuing");
                    setTimeout(function() {
                        saveNextBatch(batch_end);
                    }, 10000);
                }
            });

        });

    // });
}

function printCallback(err, result) {
    if (err) {
        winston.error(err);
    } else {
        winston.info(result);
    }
}