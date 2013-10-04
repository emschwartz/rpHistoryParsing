var couchdb = require('felix-couchdb'),
    client = couchdb.createClient(5984, 'localhost'),
    db = client.db('rpparsedhistory'),
    queue = require('queue-async');

var winston = require('winston');


db.getDoc("cc0a5edcc25f1a1ec960c1dc8a2a2030", function(err, doc){
    if (err) {
        winston.error(err);
        return;
    }

    winston.info(doc);
});
