var couchdb = require('felix-couchdb'),
    client = couchdb.createClient(5984, 'localhost'),
    db = client.db('rphistory'),
    queue = require('queue-async'),
    async = require('async'),
    _ = require('lodash');

var RippledQuerier = require('./rippledquerier'),
    rq = new RippledQuerier();

var winston = require('winston');

var MAX_ITERATORS = 1000;
var BATCH_SIZE = 1000;


// db.getDoc("cc0a5edcc25f1a1ec960c1dc8a2a2030", function(err, doc){
//     if (err) {
//         winston.error(err);
//         return;
//     }

//     winston.info(doc);
// });

saveBatch(2000000);


function saveBatch (batch_start, callback) {
    rq.getLatestLedgerIndex(function(err, latest_ledger_index){

        var batch_end = Math.min(latest_ledger_index, batch_start + BATCH_SIZE);
        winston.info("Saving batch from", batch_start, "to", batch_end);
        var incides = _.range(batch_start, batch_end);

        rq.getLedgerRange(batch_start, batch_end, function(err, ledgers){
            if (err) {
                winston.error("Error saving batch from", batch_start, "to", batch_end, ":", err);
                return;
            }

            async.eachLimit(ledgers, MAX_ITERATORS, function(ledger, async_callback){

                db.saveDoc(ledger.ledger_index, ledger, function(err, ok){
                    if (err) {
                        async_callback(err);
                        return;
                    }
                    async_callback(null);
                });

            }, function(err){
                if (err) {
                    winston.error("Error saving batch from", batch_start, "to", batch_end, ":", err);
                    return;
                }

                callback(null, batch_end);
            });

        });

    });
}

// rq.getLedger(ledger_index, function(err, ledger){
//     if (err) {
//         winston.error(err);
//         return;
//     }

//     db.saveDoc(ledger_index, ledger, function(err, ok){
//         if (err) {
//             winston.error(err);
//             return;
//         }
//         winston.info("Ledger", ledger_index, "saved to CouchDB");
//     });

// });

