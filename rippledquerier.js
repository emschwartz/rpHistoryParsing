var sqlite3 = require('sqlite3').verbose(),
    winston = require('winston'),
    path = require('path'),
    ripple = require('ripple-lib'),
    knox = require('knox'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async');


var config = require('./config');


var testrq = new RippledQuerier("/ripple/server/db");
testrq.getLedger(20000000);


var RippledQuerier = (function(db_url) {

    var rq = {};

    var txdb = new sqlite3.Database(path.resolve(config.dbPath || ".", 'transaction.db'));
    var ledb = new sqlite3.Database(path.resolve(config.dbPath || ".", 'ledger.db'));

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
                    callback(err);
                    return;
                }

                if (ledb_rows.length === 0) {
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

        txdb.all("SELECT * FROM Ledgers WHERE LedgerSeq = ?;", [ledger_index],
            function(err, rows) {
                if (err) {
                    callback(err);
                    return;
                }

                callback(null, rows);
            });
    }

    rq.getLedger = function(ledger_index, callback) {

        getRawLedger(callback);
        getRawTxForLedger(callback);

    };

    return rq;

}());