var winston = require('winston'),
    async = require('async'),
    _ = require('lodash');

var config = require('./config');
var db = require('nano')('http://' + config.couchdb.username + ':' + config.couchdb.password + '@' + config.couchdb.host + ':' + config.couchdb.port + '/' + config.couchdb.database);

var RippledQuerier = require('./rippledquerier'),
    rq = new RippledQuerier();

var MAX_ITERATORS = 1000;
var BATCH_SIZE = 1000;



// run with no arguments or with ledger index to start from
if (process.argv.length < 3) {

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

        if (res && res.results && res.results.length > 0) {

            for (var r = 0; r < res.results.length; r++) {
                if (parseInt(res.results[r].id, 10) > 0) {
                    last_saved_index = parseInt(res.results[r].id, 10) - BATCH_SIZE * 10;  
                    // go back further in case there was a problem and the last batch wasn't saved properly
                    if (last_saved_index < 32570)
                        last_saved_index = 32569;
                    break;
                }
            }    
        } else {
            last_saved_index = 32569;
        }

        winston.info("Starting from last saved index:", last_saved_index);

        saveNextBatch(last_saved_index + 1);
        return;
    });

} else if (process.argv.length === 3) {

    var last_saved_index = parseInt(process.argv[2]);
    saveNextBatch(last_saved_index);
    return;

}

function addLeadingZeros (number, digits) {
    if (typeof digits === "undefined")
        digits = 10;
    var num_str = String(number);
    while(num_str.length < digits) {
        num_str = "0" + num_str;
    }
    return num_str;
}


function saveNextBatch(batch_start, previous_ledger_hash) {

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

            // TODO check that each ledger's ledger_hash === the previous_hash of the following ledger
            ledgers.sort(function(a, b){
                return a.ledger_index - b.ledger_index;
            });

            var previous_hash, start_index;
            if (previous_ledger_hash) {
                previous_hash = previous_ledger_hash;
                start_index = 0;
            } else {
                previous_hash = ledgers[0].ledger_hash;
                start_index = 1;
            }
            
            for (var led = start_index, len = ledgers.length; led < len; led++) {
                if (ledgers[led].parent_hash !== previous_hash)
                    throw(new Error("Error in chain of ledger hashes:" + 
                                    "\n  Previous Ledger Hash: " + previous_hash + 
                                    "\n  This Ledger's Parent Hash: " + ledgers[led].parent_hash + 
                                    "\n  Ledger: " + JSON.stringify(ledgers[led])));
                else
                    previous_hash = ledgers[led].ledger_hash;
            }

            var last_ledger_hash = ledgers[ledgers.length - 1].ledger_hash;


            // list docs to get couchdb _rev to update docs already in db
            db.list({
                startkey: addLeadingZeros(batch_start),
                endkey: addLeadingZeros(batch_end)
            }, function(err, res){

                var docs = _.map(ledgers, function(ledger) {
                    var led_num = String(ledger.ledger_index);
                    var id = addLeadingZeros(led_num, 10);
                    ledger._id = id;
                    return ledger;
                });

                if (err || res && res.rows && res.rows.length > 0) {
                    _.each(res.rows, function(row){
                        var id = row.id,
                            rev = row.value.rev;

                        if (parseInt(id, 10) - batch_start > 0 
                            && parseInt(id, 10) - batch_start < docs.length
                            && docs[parseInt(id, 10) - batch_start]._id === id) {
                            docs[parseInt(id, 10) - batch_start]._rev = rev;
                        } else {
                            var doc_index = _.findIndex(docs, function(doc){
                                return doc._id === id;
                            });
                            if (doc_index >= 0)
                                docs[doc_index]._rev = rev;
                        }
                    });
                }

                // bulk update/add docs
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
                            saveNextBatch(batch_end, last_ledger_hash);
                        });
                    else {
                        // winston.info("Only got", (batch_end - batch_start), "ledgers, waiting 10 sec before continuing");
                        setTimeout(function() {
                            saveNextBatch(batch_end, last_ledger_hash);
                        }, 10000);
                    }
                });
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