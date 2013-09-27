var sqlite3 = require('sqlite3').verbose(),
    winston = require('winston'),
    path = require('path'),
    ripple = require('ripple-lib'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async');


var config = require('./config');


var FIRST_LEDGER = 32570;
var FIRST_CLOSING_TIME = 410325670;


// PRIVATE FUNCTIONS

function printCallback(err, result) {
    if (err) {
        winston.error(err);
    } else {
        winston.info(result);
    }
}

function rpEpochFromTimestamp(timestamp) {
    return timestamp / 1000 - 0x386D4380;
}

function getRawLedger(dbs, ledger_index, callback) {
    if (!callback) callback = printCallback;

    // winston.info("getting raw ledger", ledger_index);

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

            if (rows.length > 1) {
                winston.error("dbs.ledb has more than 1 entry for ledger_index:", ledger_index, "continuing anyway");
            }

            // winston.info("for ledger", ledger_index, "got", rows);

            if (!callback) winston.error("callback is not defined");
            callback(null, rows[0]);
        });
}

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

function parseLedger(raw_ledger, raw_txs, callback) {

    // winston.info("Parsing ledger:", raw_ledger.LedgerSeq, "which has this many txs:", raw_txs.length);

    var ledger;

    try {
        ledger = {
            accepted: true,
            account_hash: raw_ledger.AccountSetHash,
            close_time_rpepoch: raw_ledger.ClosingTime,
            close_time_timestamp: ripple.utils.toTimestamp(raw_ledger.ClosingTime),
            close_time_human: moment(ripple.utils.toTimestamp(raw_ledger.ClosingTime)).format("YYYY-MM-DD HH:mm:ss Z"),
            close_time_resolution: raw_ledger.CloseTimeRes,
            closed: true,
            hash: raw_ledger.LedgerHash,
            ledger_hash: raw_ledger.LedgerHash,
            ledger_index: raw_ledger.LedgerSeq,
            parent_hash: raw_ledger.PrevHash,
            total_coins: raw_ledger.TotalCoins,
            transaction_hash: raw_ledger.TransSetHash
        };

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

            parsed_tx.metaData = parsed_meta;
            return parsed_tx;

        });

        ledger.transactions = transactions;
        callback(null, ledger);

    } catch (err) {
        winston.error("Error parsing ledger:", raw_ledger.LedgerSeq);
        callback(err);
    }

}

function getLedger(dbs, ledger_index, callback) {
    if (!callback) callback = printCallback;

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

            parseLedger(raw_ledger, raw_txs, callback);

        });
    });
}

function getLedgerRange(dbs, start, end, max_iterators, callback) {
    if (!callback) callback = printCallback;

    var indices = _.range(start, end);

    winston.info("getting ledger range from:", start, "to", end, "max_iterators", max_iterators);

    async.mapLimit(indices, max_iterators, function(ledger_index, async_callback){
        getLedger(dbs, ledger_index, async_callback);
    }, function(err, ledgers) {
        if (err) {
            winston.error("Error getting ledger range:", err);
            callback(err);
            return;
        }
        callback(null, ledgers);
    });

}


function getRawLedgersForEpochRange(dbs, start_epoch, end_epoch, max_iterators, callback) {
    if (!callback) callback = printCallback;

    if (end_epoch < start_epoch) {
        var temp = end_epoch;
        end_epoch = start_epoch;
        start_epoch = temp;
    }

    searchLedgerByClosingTime(dbs, start_epoch, function(err, start_index) {
        if (err) {
            callback(err);
            return;
        }

        winston.info("start_epoch", start_epoch, "start_index", start_index);

        searchLedgerByClosingTime(dbs, end_epoch, function(err, end_index) {
            if (err) {
                callback(err);
                return;
            }

            winston.info("end_epoch", end_epoch, "end_index", end_index);

            getLedgerRange(dbs, start_index, end_index + 1, max_iterators, callback);

        });

    });

}

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

        getRawLedger(dbs, latest_index, function(err, latest_ledger){
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
            winston.info("Got here");
            callback(err);
            return;
        }

        for (var i = 0; i < rows.length - 1; i++) {
            // winston.info("rows[i][index]",rows[i][index], "rows[i][key]", rows[i][key], "val", val, "rows[i][index]", rows[i][index], "rows[i + 1][key]", rows[i + 1][key]);
            if (rows[i][key] <= val && val < rows[i + 1][key]) {
                dbRecursiveSearch(db, table, index, rows[i][index], rows[i + 1][index], key, val, callback);
                return;
            }
        }
        callback(new Error("Error in recursive search"));
    });

}




// EXPORTS

function RippledQuerier(max_iterators) {

    if (!max_iterators)
        max_iterators = 1000;

    var dbs = {
        ledb: new sqlite3.Database(path.resolve(config.dbPath || "/ripple/server/db", 'ledger.db')),
        txdb: new sqlite3.Database(path.resolve(config.dbPath || "/ripple/server/db", 'transaction.db'))
    };

    var rq = {};

    rq.FIRST_LEDGER = FIRST_LEDGER;
    rq.FIRST_CLOSING_TIME = FIRST_CLOSING_TIME;

    rq.getLatestLedgerIndex = function(dbs, callback) {
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

    rq.getLedgersByRpEpochRange = function(rp_start, rp_end, callback) {
        if (!callback) callback = printCallback;

        getRawLedgersForEpochRange(dbs, rp_start, rp_end, max_iterators, function(err, raw_ledgers) {
            if (err) {
                callback(err);
                return;
            }

            async.mapLimit(raw_ledgers, max_iterators, function(raw_ledger, async_callback) {

                var ledger_index = raw_ledger.LedgerSeq;
                getRawTxForLedger(dbs, ledger_index, function(err, raw_txs) {
                    if (err) {
                        async_callback(err);
                        return;
                    }

                    parseLedger(raw_ledger, raw_txs, async_callback);

                });

            }, function(err, results) {
                if (err) {
                    callback(err);
                    return;
                }

                callback(null, results);

            });
        });
    };

    rq.getLedgersByTimeRange = function(start, end, callback) {
        if (!callback) callback = printCallback;

        var start_moment = moment(start);
        var end_moment = moment(end);

        var start_rpepoch = rpEpochFromTimestamp(start_moment.valueOf());
        var end_rpepoch = rpEpochFromTimestamp(end_moment.valueOf());

        this.getLedgersByRpEpochRange(start_rpepoch, end_rpepoch, callback);
    };

    return rq;

}


// TESTS

// var testrq = new RippledQuerier();
// testrq.getLedgersByRpEpochRange(431582650, 431582680, function(err, ledgers){
//     if (err) {
//         winston.error(err);
//         return;
//     }
//     winston.info("Got rq many ledgers:", ledgers.length);
// });



module.exports.RippledQuerier = RippledQuerier;