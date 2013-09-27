var winston = require('winston'),
    ripple = require('ripple-lib'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    knox = require('knox');

var client = knox.createClient({
    key: config.s3.key,
    secret: config.s3.secret,
    bucket: config.s3.bucket
});

var rq = new RippledQuerier({max_iterators: 100});



function getLedgerManifest(callback) {

    var req = client.get('/meta/ledger-manifest.json').on('response', function(res) {

        var manifest = '';

        res.on('data', function(chunk) {
            manifest += chunk;
        });

        res.on('end', function() {
            var parsed_manifest = JSON.parse(manifest);
            callback(null, parsed_manifest);
        });

        res.on('error', function(err) {
            winston.info("Error getting manifest, creating a new one");
            callback(err);
        });

    }).end();

}