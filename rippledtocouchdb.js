var couchdb = require('felix-couchdb'),
    client = couchdb.createClient(5984, 'localhost'),
    db = client.db('rpparsedhistory'),
    queue = require('queue-async');


db.