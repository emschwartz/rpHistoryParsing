var sqlite3 = require('sqlite3').verbose(),
    winston = require('winston'),
    path = require('path'),
    ripple = require('ripple-lib'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async');


var config = require('./config');

// Can be called with params
//  max_iterators (for async functions)
//  db_path (local path to rippled server)
(function(params) {

    if (!params) params = {};

    var MAX_ITERATORS;

    if (params.max_iterators)
        MAX_ITERATORS = params.max_iterators;
    else
        MAX_ITERATORS = 100;




    var rq = {};

    rq.FIRST_LEDGER = 32570;
    rq.FIRST_CLOSING_TIME = 410325670;

    var txdb, ledb;
    ledb = new sqlite3.Database(path.resolve(params.db_path || config.dbPath || "/ripple/server/db", 'ledger.db'));
    txdb = new sqlite3.Database(path.resolve(params.db_path || config.dbPath || "/ripple/server/db", 'transaction.db'));


    function printCallback(err, result) {
        if (err) {
            winston.error(err);
        } else {
            winston.info(result);
        }
    }

    function getRawLedger(ledger_index, callback) {
        if (!callback) callback = printCallback;

        ledb.all("SELECT * FROM Ledgers WHERE LedgerSeq = ?;", [ledger_index],
            function(err, rows) {
                if (err) {
                    winston.error("Error getting raw ledger:", ledger_index);
                    callback(err);
                    return;
                }

                if (rows.length === 0) {
                    callback(new Error("ledb has no ledger of index: " + ledger_index));
                    return;
                }

                if (rows.length > 1) {
                    winston.error("ledb has more than 1 entry for ledger_index:", ledger_index, "continuing anyway");
                }

                callback(null, rows[0]);
            });
    }

    function getRawLedgersForEpochRange (start_epoch, end_epoch, callback) {
        if (!callback) callback = printCallback;

        ledb.all("SELECT * FROM Ledgers WHERE (ClosingTime >= ? and ClosingTime < ?);", [start_epoch, end_epoch],
            function(err, rows){
                if (err) {
                    callback(err);
                    return;
                }
                callback(null, rows);
            });
    }

    function getRawTxForLedger(ledger_index, callback) {
        if (!callback) callback = printCallback;

        txdb.all("SELECT * FROM Transactions WHERE LedgerSeq = ?;", [ledger_index],
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

        winston.info("Parsing ledger:", raw_ledger.LedgerSeq, "which has this many txs:", raw_txs.length);

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
            callback(err);
        }

    }


    // PUBLIC FUNCTIONS

    rq.getLatestLedgerIndex = function(callback) {
        if (!callback) callback = printCallback;

        ledb.all("SELECT LedgerSeq FROM Ledgers ORDER BY LedgerSeq DESC LIMIT 1;", function(err, rows){
            if (err) {
                callback(err);
                return;
            }
            callback(rows[0].LedgerSeq);
        });
    };

    rq.getLedger = function(ledger_index, callback) {
        if (!callback) callback = printCallback;

        getRawLedger(ledger_index, function(err, raw_ledger) {
            if (err) {
                callback(err);
                return;
            }

            getRawTxForLedger(ledger_index, function(err, raw_txs) {
                if (err) {
                    callback(err);
                    return;
                }

                parseLedger(raw_ledger, raw_txs, callback);

            });
        });
    };


    rq.getLedgerRange = function(start, end, callback) {
        if (!callback) callback = printCallback;

        var indices = _.range(start, end);

        async.mapLimit(indices, max_iterators, rq.getLedger, function(err, ledgers) {
            if (err) {
                callback(err);
                return;
            }
            callback(null, ledgers);
        });

    };

    rq.getLedgersByRpEpochRange = function(rp_start, rp_end, callback) {
        if (!callback) callback = printCallback;

        getRawLedgersForEpochRange(rp_start, rp_end, function(err, raw_ledgers){
            if (err) {
                callback(err);
                return;
            }

            async.mapLimit(raw_ledgers, max_iterators, function(raw_ledger, async_callback){

                var ledger_index = raw_ledger.LedgerSeq;
                getRawTxForLedger(ledger_index, function(err, raw_txs){
                    if (err) {
                        async_callback(err);
                        return;
                    }

                    parseLedger(raw_ledger, raw_txs, async_callback);

                });

            }, function(err, results){
                if (err) {
                    callback(err);
                    return;
                }

                callback(null, results);

            });
        });
    };

    return rq;

}());



// TESTS

// var testrq = new RippledQuerier();
// testrq.getLedgersByRpEpochRange(431582650, 431582680, function(err, ledgers){
//     if (err) {
//         winston.error(err);
//         return;
//     }
//     winston.info("Got rq many ledgers:", ledgers.length);
// });



exports.RippledQuerier = RippledQuerier;