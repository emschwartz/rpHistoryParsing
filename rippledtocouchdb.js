var couchdb = require('felix-couchdb'),
    client = couchdb.createClient(5984, 'localhost'),
    db = client.db('rpparsedhistory'),
    queue = require('queue-async');


var req = db.getDoc("aecd76d84ad9af8fdcb6c39cfb000bde");
req.on('response', function(res){
    winston.info(res);
});