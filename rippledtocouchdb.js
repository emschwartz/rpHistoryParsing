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
var BATCH_SIZE = 10000;


if (process.argv.length === 3) {
    saveNextBatch(parseInt(process.argv[2]), 10);
}

// db.view("dd1", "last_transaction", function(err, ))


function saveNextBatch (batch_start) {

    rq.getLatestLedgerIndex(function(err, latest_ledger_index){

        var batch_end = Math.min(latest_ledger_index, (batch_start + BATCH_SIZE));
        winston.info("Saving batch from", batch_start, "to", batch_end);
        var incides = _.range(batch_start, batch_end);

        rq.getLedgerRange(batch_start, batch_end, function(err, ledgers){
            if (err) {
                winston.error("Error getting batch from", batch_start, "to", batch_end, ":", err);
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
                    winston.error("Error saving batch from", batch_start, "to", batch_end, ":", JSON.stringify(err));
                    return;
                }

                saveNextBatch(batch_end);
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