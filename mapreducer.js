var winston = require('winston'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    fs = require('fs'),
    RippledQuerier = require('./rippledquerier').RippledQuerier;


var MAX_ITERATORS = 1000;

var rq = new RippledQuerier(MAX_ITERATORS);

var headers = "ledger_index, closing_time, num_transactions\n";

fs.writeFileSync("tx_history.csv", headers);

function applyToChunksOfTime(time_chunk_type, time_chunk_multiple, start_time, stop_time, iterator, callback) {

    if ()

    if (start_time > stop_time) {
        callback();
        return;
    }

    var batch_end_time = moment(start_time).add(time_chunk_type, time_chunk_multiple);

    rq.getLedgersForTimeRange(moment(start_time),
        batch_end_time,
        function(err, ledgers) {

            iterator(ledgers, function(err){
                if (err) {
                    callback(err);
                    return;
                }
                setImmediate(function(){
                    applyToChunksOfTime(time_chunk_type, time_chunk_multiple, batch_end_time, stop_time, iterator, callback);
                });
            });

        });

}



// function applyToRange (first_index, last_index, iterator, callback) {

//     if (first_index === null || first_index === "FIRST" || first_index === "first") {
//         first_index = rq.FIRST_INDEX;
//     }

//     if (last_index === null || last_index === "LAST" || last_index === "last") {
//         rq.getLatestLedgerIndex(function(err, rq_last_index){
//             if (err) {
//                 callback(err);
//                 return;
//             }
//             applyToRange(first_index, rq_last_index, iterator, callback);
//         });
//         return;
//     }

//     var indices = _.range(first_index, last_index);

//     async.eachLimit(indices, MAX_ITERATORS, function(index, async_callback){

//         rq.getLedger(index, function(err, ledger){
//             if (err) {
//                 callback(err);
//                 return;
//             }

//             iterator(ledger, async_callback);
//         });

//     }, callback);

// }

// function applyToAll (iterator, callback) {
//     applyToRange(null, null, iterator, callback);
// }





// function mapOverRange (first_index, last_index, iterator, callback) {

//     if (first_index === null || first_index === "FIRST" || first_index === "first") {
//         first_index = rq.FIRST_INDEX;
//     }

//     if (last_index === null || last_index === "LAST" || last_index === "last") {
//         rq.getLatestLedgerIndex(function(err, rq_last_index){
//             if (err) {
//                 callback(err);
//                 return;
//             }
//             mapOverRange(first_index, rq_last_index, iterator, callback);
//         });
//         return;
//     }

//     var indices = _.range(first_index, last_index);


//     async.mapLimit(indices, MAX_ITERATORS, function(index, async_callback){

//         rq.getLedger(index, function(err, ledger){
//             if (err) {
//                 callback(err);
//                 return;
//             }

//             iterator(ledger, async_callback);
//         });

//     }, callback);

// }

// function mapOverAll (iterator, callback) {
//     mapOverRange("FIRST", "LAST", iterator, callback);
// }




// applyToAll(function(ledger, async_callback){

//     var row = ledger.ledger_index + ", " + ledger.close_time_human + ", " + ledger.transactions.length + "\n";

//     fs.appendFile("tx_history.csv", row, async_callback);


// }, function(err){

//     if (err){
//         winston.error(err);
//         return;
//     }

// });




// mapOverAll(function(ledger, async_callback){

// }, function(err, results){

//     for (var r = 0, len = results.length; r < len; r++) {
//         fs.appenFile("tx_history.csv", (results[r].ledger_index + ", " + results[r].close_time_human + results[r].transactions.length))
//     }

// });


// applyToRange(2400000, "LAST", function(ledger, async_callback){

//     winston.info("Ledger", ledger.ledger_index, "has", ledger.transactions.length, "transactions");
//     async_callback();

// }, function(err){

//     if (err) {
//         winston.error(err);
//         return;
//     }

// });