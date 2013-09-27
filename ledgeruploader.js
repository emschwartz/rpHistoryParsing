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
var MAX_UPLOADERS = 25;


var rq = new RippledQuerier({
    max_iterators: 100
});


startUploadingLedgers(1000);

function startUploadingLedgers(batch_size) {

    getLastUploadedLedger(function(err, last_indiv_ledger) {
        if (err) {
            winston.error("Error getting last uploaded ledger", err);
            return;
        }

        uploadNextBatch(last_indiv_ledger, batch_size, uploadNextBatch);

    });

}

function uploadNextBatch(batch_start, batch_size) {

    rq.getLatestLedgerIndex(function(err, latest_index) {
        if (err) {
            winston.error("Error getting latest ledger index", err);
            return;
        }

        var batch_end = batch_start + batch_size;
        if (batch_end >= latest_index)
            batch_end = latest_index;

        rq.getLedgerRange(batch_start, batch_end, function(err, ledgers) {
            if (err) {
                winston.error("Error getting ledger range", err);
                return;
            }

            async.eachLimit(ledgers, MAX_UPLOADERS, uploadToS3, function(err) {
                if (err) {
                    winston.error("Error uploading ledger", err);
                } else {
                    updateS3Manifest(batch_end - 1);
                    uploadNextBatch(batch_start + batch_size, batch_size);
                }
            });

        });

    });

}


function uploadToS3(ledger, callback) {

    var ledger_str = JSON.stringify(ledger);

    var req = client.put('/ledgers/' + ledger.ledger_index + '.json', {
        'Content-Length': ledger_str.length,
        'Content-Type': 'application/json'
    });

    req.on('error', function(err) {
        winston.error("Error uploading ledger", ledger.ledger_index, "to S3", err, "trying again in 1 sec");
        setTimeout(function() {
            uploadToS3(ledger);
        }, 1000);
    });

    req.on('response', function(res) {

        res.on('error', function(err) {
            winston.error("Error uploading ledger", ledger.ledger_index, "to S3", err, "trying again in 1 sec");
            setTimeout(function() {
                uploadToS3(ledger);
            }, 1000);
        });

        if (200 === res.statusCode) {
            winston.info("Ledger", ledger.ledger_index, "saved to S3 at:", req.url);
            // updateS3Manifest(ledger.ledger_index);
            if (callback)
                callback();
        }

    });

    req.end(ledger_str);

}


function updateS3Manifest(ledger_index) {

    getLedgerManifest(function(manifest) {

        if (typeof manifest.latest_indiv_ledger !== "undefined" && ledger_index <= manifest.latest_indiv_ledger)
            return;

        manifest.latest_indiv_ledger = ledger_index;

        var manifest_str = JSON.stringify(manifest);

        var req = client.put('/meta/ledger-manifest.json', {
            'Content-Length': manifest_str.length,
            'Content-Type': 'application/json'
        });

        req.on('error', function(err) {
            winston.error("Error updating manifest");
            setTimeout(function() {
                updateS3Manifest(ledger_index);
            }, 100);
        });

        req.on('response', function(res) {

            if (200 === res.statusCode) {
                winston.info("Updated ledger manifest to latest_indiv_ledger = ",
                    ledger_index);
            }

        });

        req.end(manifest_str);


    });

}


function getLastUploadedLedger(callback) {
    getLedgerManifest(function(err, manifest) {
        if (err || !manifest.last_indiv_ledger)
            callback(null, rq.FIRST_LEDGER);
        else
            callback(null, manifest.last_indiv_ledger);
    });
}

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
            callback(err);
        });

    }).end();

}