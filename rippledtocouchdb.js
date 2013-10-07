// var couchdb = require('felix-couchdb'),
//     client = couchdb.createClient(5984, '0.0.0.0'),
//     db = client.db('rphistory'),
var nano = require('nano')('http://0.0.0.0:5984'),
    db = nano.use('rphistory');

var config = require('./config');

var async = require('async'),
    _ = require('lodash');

var RippledQuerier = require('./rippledquerier'),
    rq = new RippledQuerier();

var winston = require('winston');

var MAX_ITERATORS = 1000;
var BATCH_SIZE = 1000;


// if (process.argv.length === 3) {
//     saveNextBatch(parseInt(process.argv[2]), 10);
// }

nano.auth(config.couchdb_username, config.couchdb_password, function(err){

    if (err) {
        winston.error("Error connecting to couchdb:", err);
        return;
    }

    db.changes({
        limit: 1,
        descending: true
    }, function(err, res){
        if (err) {
            winston.error("Error getting last ledger saved");
            return;
        }

        var last_saved_index = parseInt(res.results[0].id, 10);
        saveNextBatch(last_saved_index + 1);
    });


});

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

            var docs = _.map(ledgers, function(ledger){
                ledger._id = String(ledger.ledger_index);
                return ledger;
            });

            // winston.info(JSON.stringify(docs));

            db.bulkDocs({docs: docs}, function(err){
                if (err) {
                    winston.error("Error saving batch from", batch_start, "to", batch_end, ":", JSON.stringify(err));
                    return;
                }

                if (batch_end - batch_start > 1)
                    setImmediate(function(){
                        saveNextBatch(batch_end);
                    });
                else {
                    winston.info("Only got", (batch_end - batch_start), "ledgers, waiting 10 sec before continuing");
                    setTimeout(function(){
                        saveNextBatch(batch_end);
                    }, 10000);
                }
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