var sqlite3 = require('sqlite3').verbose(),
    winston = require('winston'),
    path = require('path'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    ripple = require('ripple-lib'),
    Ledger = require('./node_modules/ripple-lib/src/js/ripple/ledger').Ledger,
    Remote = ripple.Remote;

var config = require('./config');

var FIRST_LEDGER = 32570;
var FIRST_CLOSING_TIME = 410325670;


function RippledQuerier(max_iterators) {

    if (!max_iterators)
        max_iterators = 1000;

    var dbs = {
        ledb: new sqlite3.Database(path.resolve(config.dbPath || "/ripple/server/db", 'ledger.db')),
        txdb: new sqlite3.Database(path.resolve(config.dbPath || "/ripple/server/db", 'transaction.db'))
    };

    var rq = {};

    rq.FIRST_LEDGER = FIRST_LEDGER;
    rq.FIRST_INDEX = FIRST_LEDGER;
    rq.FIRST_CLOSING_TIME = FIRST_CLOSING_TIME;


    // RippledQuerier functions

    rq.getLatestLedgerIndex = function(callback) {
        getLatestLedgerIndex(dbs, callback);
    };

    rq.getLedger = function(ledger_index, callback) {
        getLedger(dbs, ledger_index, callback);
    };

    rq.searchLedgerByClosingTime = function(rpepoch, callback) {
        searchLedgerByClosingTime(dbs, rpepoch, callback);
    };

    rq.getLedgerRange = function(start, end, callback) {
        getLedgerRange(dbs, start, end, max_iterators, callback);
    };

    rq.getLedgersForRpEpochRange = function(rp_start, rp_end, callback) {
        getLedgersForRpEpochRange(dbs, rp_start, rp_end, max_iterators, callback);
    };

    // rq.getLedgersForTimeRange gets the PARSED ledgers between the two given momentjs-readable times
    rq.getLedgersForTimeRange = function(start, end, callback) {

        var start_moment = moment(start);
        // winston.info("start_moment", start_moment.format());
        var end_moment = moment(end);
        // winston.info("end_moment", end_moment.format());

        var start_rpepoch = rpEpochFromTimestamp(start_moment.valueOf());
        // winston.info("start_rpepoch", start_rpepoch);
        var end_rpepoch = rpEpochFromTimestamp(end_moment.valueOf());
        // winston.info("end_rpepoch", end_rpepoch);

        getLedgersForRpEpochRange(dbs, start_rpepoch, end_rpepoch, max_iterators, callback);
    };

    return rq;

}



// PRIVATE FUNCTIONS


// printCallback is used as the default callback function

function printCallback(err, result) {
    if (err) {
        winston.error(err);
    } else {
        winston.info(result);
    }
}

// rpEpochFromTimestamp converts the ripple epochs to a javascript timestamp

function rpEpochFromTimestamp(timestamp) {
    return timestamp / 1000 - 0x386D4380;
}


// getRawLedger gets the raw (encoded) ledger blob from the ledb database 

function getRawLedger(dbs, ledger_index, callback) {
    if (!callback) callback = printCallback;

    // winston.info("getting raw ledger", ledger_index);
    if (!dbs) winston.error("dbs is not defined in getRawLedger");


    dbs.ledb.all("SELECT * FROM Ledgers WHERE LedgerSeq = ?;", [ledger_index],
        function(err, rows) {
            if (err) {
                winston.error("Error getting raw ledger:", ledger_index, rows);
                callback(err);
                return;
            }

            if (rows.length === 0) {
                callback(new Error("dbs.ledb has no ledger of index: " + ledger_index));
                return;
            }

            var raw_ledger;

            if (rows.length === 1) {
                raw_ledger = rows[0];
            } else if (rows.length > 1) {
                rows.sort(function(row1, row2) {
                    if (row1.TransSetHash > row2.TransSetHash) {
                        return -1;
                    } else {
                        return 1;
                    }});

                raw_ledger = rows[0];

                raw_ledger.conflicting_ledger_headers = [];
                for (var r = 1; r < rows.length; r++) {
                    if (raw_ledger.LedgerHash !== rows[r].LedgerHash);
                        raw_ledger.conflicting_ledger_headers.push(rows[r]);
                }
            }
            
            callback(null, raw_ledger);
        });
}

// getRawTxForLedger gets the raw tx blobs from the txdb database

function getRawTxForLedger(dbs, ledger_index, callback) {
    if (!callback) callback = printCallback;

    dbs.txdb.all("SELECT * FROM Transactions WHERE LedgerSeq = ?;", [ledger_index],
        function(err, rows) {
            if (err) {
                winston.error("Error getting raw txs for ledger:", ledger_index);
                callback(err);
                return;
            }

            callback(null, rows);
        });
}

// parseLedger parses the raw ledger and associated raw txs into a single json ledger

function parseLedger(raw_ledger, raw_txs, callback) {

    var ledger;

    ledger = {
        // accepted: true,
        account_hash: raw_ledger.AccountSetHash,
        close_time_rpepoch: raw_ledger.ClosingTime,
        close_time_timestamp: ripple.utils.toTimestamp(raw_ledger.ClosingTime),
        close_time_human: moment(ripple.utils.toTimestamp(raw_ledger.ClosingTime)).utc().format("YYYY-MM-DD HH:mm:ss Z"),
        close_time_resolution: raw_ledger.CloseTimeRes,
        // closed: true,
        ledger_hash: raw_ledger.LedgerHash,
        ledger_index: raw_ledger.LedgerSeq,
        parent_hash: raw_ledger.PrevHash,
        total_coins: raw_ledger.TotalCoins,
        transaction_hash: raw_ledger.TransSetHash
    };

    if (raw_txs !== null) {
        var transactions = _.map(raw_txs, function(raw_tx) {

            // Parse tx
            var tx_buffer = new Buffer(raw_tx.RawTxn);
            var tx_buffer_array = [];
            for (var i = 0, len = tx_buffer.length; i < len; i++) {
                tx_buffer_array.push(tx_buffer[i]);
            }
            var tx_serialized_obj = new ripple.SerializedObject(tx_buffer_array);
            var parsed_tx = tx_serialized_obj.to_json();

            // Parse metadata
            var meta_buffer = new Buffer(raw_tx.TxnMeta);
            var meta_buffer_array = [];
            for (var j = 0, len2 = meta_buffer.length; j < len2; j++) {
                meta_buffer_array.push(meta_buffer[j]);
            }
            var meta_serialized_obj = new ripple.SerializedObject(meta_buffer_array);
            var parsed_meta = meta_serialized_obj.to_json();

            for (var n = 0, nlen = parsed_meta.AffectedNodes.length; n < nlen; n++) {
                var node = parsed_meta.AffectedNodes[n].CreatedNode 
                            || parsed_meta.AffectedNodes[n].ModifiedNode
                            || parsed_meta.AffectedNodes[n].DeletedNode;
                if (node.LedgerEntryType === "Offer") {

                    var BookDirectory; 
                    if (node.hasOwnProperty("FinalFields"))
                        BookDirectory = node.FinalFields.BookDirectory;
                    else if (node.hasOwnProperty("NewFields"))
                        BookDirectory = node.NewFields.BookDirectory;

                    if (typeof BookDirectory === "string") {
                        var exchange_rate = ripple.Amount.from_quality(BookDirectory).to_json();
                        node.exchange_rate = exchange_rate.value;
                    }
                }
            }

            parsed_tx.metaData = parsed_meta;
            return parsed_tx;

        });

        ledger.transactions = transactions;
    }

    // store conflicting headers if there are multiple headers for a given ledger_index
    if (raw_ledger.conflicting_ledger_headers 
        && raw_ledger.conflicting_ledger_headers.length > 0) {

        ledger.conflicting_ledger_headers = [];

        raw_ledger.conflicting_ledger_headers.forEach(function(head){

            ledger.conflicting_ledger_headers.push(parseLedger(head, null));

        });

    }

    // check that transaction hash is correct
    var ledger_json_hash = Ledger.from_json(ledger).calc_tx_hash().to_hex();
    if (ledger_json_hash === ledger.transaction_hash) {

        callback(null, ledger);
    
    } else {

        // try getting ledger with API call instead of from rippled database

        winston.info("Trying to connect to ripple-lib remote");

        var remote = new Remote({
            servers: [{
                host:    's1.ripple.com',
                port:    443,
                secure:  true
            }]
        });

        remote.connect(function() {
            
            winston.info("Querying rippled for ledger:", ledger.ledger_index);
            remote.request_ledger(ledger.ledger_index, { transactions: true, expand: true }, function(err, res){

                if (err) {
                    winston.error("Error getting ledger from rippled:", err);
                    callback(err);
                    return;
                }

                // add/edit fields that aren't in rippled's json format
                var ledger = res.ledger;
                ledger.close_time_rpepoch = ledger.close_time;
                ledger.close_time_timestamp = ripple.utils.toTimestamp(ledger.close_time);
                ledger.close_time_human = moment(ripple.utils.toTimestamp(ledger.close_time)).utc().format("YYYY-MM-DD HH:mm:ss Z");

                // add exchange rate field to metadata entries
                ledger.transactions.forEach(function(transaction){
                    transaction.metaData.AffectedNodes.forEach(function(affNode){
                        var node = affNode.CreatedNode 
                            || affNode.ModifiedNode
                            || affNode.DeletedNode;

                        if (node.LedgerEntryType === "Offer") {

                            var BookDirectory; 
                            if (node.hasOwnProperty("FinalFields")) {
                                BookDirectory = node.FinalFields.BookDirectory;
                            } else if (node.hasOwnProperty("NewFields")) {
                                BookDirectory = node.NewFields.BookDirectory;
                            }

                            if (typeof BookDirectory === "string") {
                                var exchange_rate = ripple.Amount.from_quality(BookDirectory).to_json();
                                node.exchange_rate = exchange_rate.value;
                            }
                        }
                    });
                });


                var ledger_json_hash = Ledger.from_json(ledger).calc_tx_hash().to_hex();
                if (ledger_json_hash === ledger.transaction_hash) {

                    callback(null, ledger);

                } else {
                    callback(new Error("Hash of parsed ledger does not match hash in ledger header." + 
                            "\n  Actual:   " + ledger_json_hash + 
                            "\n  Expected: " + ledger.transaction_hash + 
                            "\n  Ledger: " + JSON.stringify(ledger)));
                }
                

            });

        });

        
    }

}

// getLedger gets the PARSED ledger (and associated transactions) corresponding to the ledger_index

function getLedger(dbs, ledger_index, callback) {
    if (!callback) callback = printCallback;

    if (!dbs) winston.error("dbs is not defined in getLedger");


    getRawLedger(dbs, ledger_index, function(err, raw_ledger) {
        if (err) {
            winston.error("Error getting raw ledger", ledger_index, "err", err);
            callback(err);
            return;
        }

        getRawTxForLedger(dbs, ledger_index, function(err, raw_txs) {
            if (err) {
                winston.error("Error getting raw tx for ledger", ledger_index);
                callback(err);
                return;
            }

            parseLedger(raw_ledger, raw_txs, function(err, parsed_ledger){
                if (err) {
                    winston.error("Error parsing ledger:", err);
                    callback(err);
                    return;
                }
                callback(null, parsed_ledger);
            });

        });
    });
}

// getLedgerRange gets the PARSED ledgers for the given range of indices
function getLedgerRange(dbs, start, end, max_iterators, callback) {
    if (!callback) callback = printCallback;

    if (!dbs) {
        winston.error("dbs is not defined in getLedgerRange");
        return;
    }

    var indices = _.range(start, end);

    // winston.info("getting ledger range from:", start, "to", end, "max_iterators", max_iterators);

    async.mapLimit(indices, max_iterators, function(ledger_index, async_callback) {
        getLedger(dbs, ledger_index, async_callback);
    }, function(err, ledgers) {
        if (err) {
            winston.error("Error getting ledger range:", err);
            callback(err);
            return;
        }

        if (ledgers.length === 0)
            winston.info("getLedgerRange got 0 ledgers for range", start, end);

        callback(null, ledgers);
    });

}

// getLedgersForRpEpochRange gets the PARSED ledgers that closed between the given ripple epoch times

function getLedgersForRpEpochRange(dbs, start_epoch, end_epoch, max_iterators, callback) {
    if (!callback) callback = printCallback;

    if (end_epoch < start_epoch) {
        var temp = end_epoch;
        end_epoch = start_epoch;
        start_epoch = temp;
    }

    if (start_epoch < FIRST_CLOSING_TIME)
        start_epoch = FIRST_CLOSING_TIME;

    searchLedgerByClosingTime(dbs, start_epoch, function(err, start_index) {
        if (err) {
            callback(err);
            return;
        }

        // winston.info("start_epoch", start_epoch, "start_index", start_index);

        searchLedgerByClosingTime(dbs, end_epoch, function(err, end_index) {
            if (err) {
                callback(err);
                return;
            }

            // winston.info("end_epoch", end_epoch, "end_index", end_index);

            getLedgerRange(dbs, start_index, end_index + 1, max_iterators, callback);

        });

    });

}


// getLatestLedgerIndex gets the most recent ledger index in the ledger db

function getLatestLedgerIndex(dbs, callback) {
    if (!callback) callback = printCallback;

    dbs.ledb.all("SELECT LedgerSeq FROM Ledgers ORDER BY LedgerSeq DESC LIMIT 1;", function(err, rows) {
        if (err) {
            callback(err);
            return;
        }
        callback(null, rows[0].LedgerSeq);
    });
}


// searchLedgerByClosingTime finds the ledger index of the ledger that closed nearest to the given rpepoch

function searchLedgerByClosingTime(dbs, rpepoch, callback) {
    if (!callback) callback = printCallback;

    if (rpepoch < FIRST_CLOSING_TIME) {
        callback(null, FIRST_LEDGER);
        return;
    }

    getLatestLedgerIndex(dbs, function(err, latest_index) {
        if (err) {
            callback(err);
            return;
        }

        getRawLedger(dbs, latest_index, function(err, latest_ledger) {
            if (err) {
                callback(err);
                return;
            }

            if (rpepoch >= latest_ledger.ClosingTime) {
                callback(null, latest_index);
                return;
            }

            dbRecursiveSearch(dbs.ledb, "Ledgers", "LedgerSeq", FIRST_LEDGER, latest_index, "ClosingTime", rpepoch, callback);


        });

    });

}


// dbRecursiveSearch is like a binary search but with 20 divisions each time instead of 2
// (because querying the db is slower than iterating through 20 results)

function dbRecursiveSearch(db, table, index, start, end, key, val, callback) {
    if (!callback) callback = printCallback;

    var num_queries = 20;

    if (end - start <= num_queries) {
        var query_str_final = "SELECT " + index + " FROM " + table + " " +
            "WHERE (" + index + ">=" + start + " " +
            "and " + index + "<" + end + " " +
            "and " + key + "<=" + val + ") " +
            "ORDER BY ABS(" + key + "-" + val + ") ASC;";
        db.all(query_str_final, function(err, rows) {
            // winston.info("search got:", rows[0]);
            callback(err, rows[0][index]);
        });
        return;
    }

    var indices = _.map(_.range(num_queries), function(segment) {
        return start + segment * Math.floor((end - start) / num_queries);
    });
    indices.push(end);

    var index_str = '';
    _.each(indices, function(index) {
        index_str += (index + ", ");
    });
    index_str = index_str.substring(0, index_str.length - 2);

    var query_str_recur = "SELECT * FROM " + table + " " +
        "WHERE " + index + " IN (" + index_str + ") " +
        "ORDER BY " + index + " ASC;";

    db.all(query_str_recur, function(err, rows) {

        if (err) {
            callback(err);
            return;
        }

        for (var i = 0; i < rows.length - 1; i++) {
            // winston.info("rows[i][index]",rows[i][index], "rows[i][key]", rows[i][key], "val", val, "rows[i][index]", rows[i][index], "rows[i + 1][key]", rows[i + 1][key]);
            if (rows[i][key] <= val && val < rows[i + 1][key]) {
                setImmediate(function() {
                    dbRecursiveSearch(db, table, index, rows[i][index], rows[i + 1][index], key, val, callback);
                });
                return;
            }
        }
        callback(new Error("Error in recursive search"));
    });

}


module.exports = RippledQuerier;