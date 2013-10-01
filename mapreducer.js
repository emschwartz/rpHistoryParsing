var winston = require('winston'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    RippledQuerier = require('./rippledquerier').RippledQuerier;


var MAX_ITERATORS = 1000;

var rq = new RippledQuerier(MAX_ITERATORS);

function applyToRange (first_index, last_index, iterator, callback) {

    if (first_index === null || first_index === "FIRST" || first_index === "first") {
        first_index = rq.FIRST_INDEX;
    }

    if (last_index === null || last_index === "LAST" || last_index === "last") {
        rq.getLatestLedgerIndex(function(last_index){
            applyToRange(first_index, last_index, iterator, callback);
            return;
        });
    }

    winston.info("applying to ledgers from", first_index, "to", last_index);

    var indices = _.range(first_index, last_index);

    async.each(indices, function(index, async_callback){

        rq.getLedger(index, function(err, ledger){
            if (err) {
                callback(err);
                return;
            }

            iterator(ledger, async_callback);
        });

    }, callback);

}

function applyToAll (iterator, callback) {
    applyToRange(null, null, iterator, callback);
}



applyToRange(2500000, "LAST", function(ledger, async_callback){

    winston.info("Ledger", ledger.ledger_index, "has", ledger.transactions.length, "transactions");
    async_callback();

}, function(err){

    if (err) {
        winston.error(err);
        return;
    }

});