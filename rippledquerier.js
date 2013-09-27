var sqlite3 = require('sqlite3').verbose(),
    winston = require('winston'),
    path = require('path'),
    ripple = require('ripple-lib'),
    // knox = require('knox'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async');


// var config = require('./config');


var RippledQuerier = function(db_url) {

    var rq = {};

    var txdb, ledb;

    function connectToDb(callback) {
        if (txdb && ledb) return;

        winston.info("Connecting to db");
        txdb = new sqlite3.Database(db_url || path.resolve(config.dbPath || ".", 'transaction.db'), function(err) {
            if (err) throw err;
            winston.info("txdb connected");
            ledb = new sqlite3.Database(db_url || path.resolve(config.dbPath || ".", 'ledger.db'), function(err) {
                if (err) throw err;
                winston.info("ledb connected");
                callback();
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

    function parseLedger(raw_ledger, raw_txs) {

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
        } catch (led_err) {
            winston.error("Error parsing ledger", raw_ledger.LedgerSeq);
            throw led_err;
        }

        try {
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
            return ledger;

        } catch (tx_err) {
            winston.error("Error parsing transaction");
            throw tx_err;
        }

    }

    rq.getLedger = function(ledger_index, callback) {
        if (!callback) callback = printCallback;

        connectToDb(function() {

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

                    var parsed_ledger;

                    try {
                        parsed_ledger = parseLedger(raw_ledger, raw_txs);
                    } catch (parsing_err) {
                        callback(parsing_err);
                        return;
                    }

                    callback(null, parsed_ledger);
                });
            });
        });
    };


    rq.getLedgerRange = function(start, end, callback){
        if (!callback) callback = printCallback;

        var indices = _.range(start, end);

        async.mapLimit(indices, MAX_ITERATORS, getLedger, function(err, ledgers){
            if (err) {
                callback(err);
                return;
            }
            callback(null, ledgers);
        });

    };



    

    return rq;

};



// TESTS

var testrq = new RippledQuerier("/ripple/server/db");
testrq.getLedgerRange(2000000, 2000009);



exports.RippledQuerier = RippledQuerier;