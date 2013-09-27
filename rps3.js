var winston = require('winston'),
    ripple = require('ripple-lib'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    knox = require('knox');

var config = require('./config');


var MAX_UPLOADERS = 25;


function RpS3Uploader () {

    var rps3 = {};

    rps3.uploadToS3 = function(ledger, callback) {

    // winston.info("uploading ledger to s3:", ledger.ledger_index);

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

};


rps3.updateS3Manifest = function (ledger_index) {

    // winston.info("updating s3 manifest, latest_indiv_ledger:", ledger_index);

    getLedgerManifest(function(manifest) {

        if (!manifest)
            manifest = {};

        if (manifest.latest_indiv_ledger !== "undefined" && ledger_index <= manifest.latest_indiv_ledger)
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

};


rps3.getLastUploadedLedger = function(callback) {

    winston.info("getting last uploaded ledger");

    getLedgerManifest(function(err, manifest) {
        if (err || !manifest.latest_indiv_ledger) {
            winston.info("first ledger:", rq.FIRST_LEDGER);
            callback(null, rq.FIRST_LEDGER);
        } else {
            winston.info("last uploaded ledger:", manifest.latest_indiv_ledger);
            callback(null, manifest.latest_indiv_ledger);
        }
    });
};

rps3.getLedgerManifest = function(callback) {

    winston.info("getting ledger manifest");

    var req = client.get('/meta/ledger-manifest.json');

    req.on('response', function(res) {

        var manifest = '';

        if (res.statusCode !== 200) {
            callback(new Error("Error downloading ledger manifest"));
            return;
        }

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

    });

    req.on('error', function(err) {
            callback(err);
    });

    req.end();

}

}

