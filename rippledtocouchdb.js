var couchdb = require('felix-couchdb'),
    client = couchdb.createClient(5984, 'localhost'),
    db = client.db('rpparsedhistory'),
    queue = require('queue-async');


winston.info(db.getDoc("aecd76d84ad9af8fdcb6c39cfb000bde"));
