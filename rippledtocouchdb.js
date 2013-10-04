var couchdb = require('felix-couchdb'),
    client = couchdb.createClient(5984, 'localhost'),
    db = client.db('rphistory'),
    queue = require('queue-async');

var RippledQuerier = require('./rippledquerier'),
    rq = new RippledQuerier();

var winston = require('winston');


// db.getDoc("cc0a5edcc25f1a1ec960c1dc8a2a2030", function(err, doc){
//     if (err) {
//         winston.error(err);
//         return;
//     }

//     winston.info(doc);
// });

var ledger_index = 2000000;

rq.getLedger(ledger_index, function(err, ledger){
    if (err) {
        winston.error(err);
        return;
    }

    db.saveDoc(ledger_index, ledger, function(err, ok){
        if (err) {
            winston.error(err);
            return;
        }
        winston.info("Ledger", ledger_index, "saved to CouchDB");
    });

});

