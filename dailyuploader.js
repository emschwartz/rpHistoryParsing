var winston = require('winston'),
    ripple = require('ripple-lib'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    knox = require('knox'),
    RippledQuerier = require('./rippledquerier').RippledQuerier;

var config = require('./config');

var client = knox.createClient({
    key: config.s3.key,
    secret: config.s3.secret,
    bucket: config.s3.bucket
});
var MAX_UPLOADERS = 25;
var MAX_ITERATORS = 1000;


var FIRST_DAY_STR = "2012-12-31";
var MIDNIGHT_UTC = "T00:00:00+00:00";
var FIRST_DAY = moment(FIRST_DAY_STR + MIDNIGHT_UTC);




// Run the script
var rq = new RippledQuerier(MAX_ITERATORS);
startClusteringDays();







function startClusteringDays() {

    getLastUploadedDailyPackage(function(err, prev_day_str) {
        if (err) {
            winston.error("Error geting last daily package:", err);
            return;
        }

        clusterAndUploadNextDay(prev_day_str);

    });

}

function clusterAndUploadNextDay(prev_day_str) {

    var this_day = moment(prev_day_str + MIDNIGHT_UTC).add("days", 1);

    winston.info("Clustering", this_day.format());


    getNextDay(this_day, function(err, ledgers) {
        if (err) {
            winston.error("Error geting daily ledgers:", err);
            return;
        }

        winston.info("Got this many ledgers:", ledgers.length);

        if (ledgers.length === 0) {
            setImmediate(function() {
                clusterAndUploadNextDay(this_day.format("YYYY-MM-DD"));
            });
            return;
        }


        packageDay(ledgers, function(err, daily_package) {
            if (err) {
                winston.error("Error packaging daily ledgers:", err);
                return;
            }


            uploadToS3(this_day.format("YYYY-MM-DD"), daily_package, function(err, daily_package_str) {
                if (err) {
                    winston.error("Error uploading daily ledgers:", err);
                    return;
                }

                setImmediate(function() {
                    clusterAndUploadNextDay(daily_package_str);
                });

            });


        });

    });
}


function getNextDay(start_day, callback) {

    rq.getLedgersForTimeRange(moment(start_day), moment(start_day).add('days', 1), function(err, ledgers) {
        if (err) {
            winston.error("Error getting day of ledgers", err);
            callback(err);
            return;
        }

        winston.info("getNextDay got this many ledgers:", ledgers.length);

        callback(null, ledgers);

    });
}

function packageDay(ledgers, callback) {
    async.reduce(ledgers, '', function(daily_txt, ledger, async_callback) {
        async_callback(null, (daily_txt + JSON.stringify(ledger) + '\n'));
    }, callback);
}

function uploadToS3(day_str, daily_package, callback) {

    // winston.info("uploading ledger to s3:", ledger.ledger_index);

    var daily_package_str = JSON.stringify(daily_package);

    var req = client.put('/daily-packages/' + day_str + '.json', {
        'Content-Length': daily_package_str.length,
        'Content-Type': 'application/json'
    });

    req.on('error', function(err) {
        winston.error("Error uploading daily package", day_str, "to S3", err, "trying again in 1 sec");
        setTimeout(function() {
            uploadToS3(day_str, daily_package, callback);
        }, 1000);
    });

    req.on('response', function(res) {

        res.on('error', function(err) {
            winston.error("Error uploading daily package", day_str, "to S3", err, "trying again in 1 sec");
            setTimeout(function() {
                uploadToS3(day_str, daily_package, callback);
            }, 1000);
        });

        if (200 === res.statusCode) {
            winston.info("Daily package", day_str, "saved to S3 at:", req.url);
            updateS3Manifest(day_str);
            if (callback)
                callback(null, day_str);
        }

    });

    req.end(daily_package_str);

}


function updateS3Manifest(latest_daily_package) {

    getLedgerManifest(function(manifest) {

        if (!manifest)
            manifest = {};

        if (manifest.latest_daily_package !== "undefined" && latest_daily_package <= manifest.latest_daily_package)
            return;

        manifest.latest_daily_package = latest_daily_package;

        var manifest_str = JSON.stringify(manifest);

        var req = client.put('/meta/ledger-manifest.json', {
            'Content-Length': manifest_str.length,
            'Content-Type': 'application/json'
        });

        req.on('error', function(err) {
            winston.error("Error updating manifest");
            setTimeout(function() {
                updateS3Manifest(latest_daily_package);
            }, 100);
        });

        req.on('response', function(res) {

            if (200 === res.statusCode) {
                winston.info("Updated ledger manifest to latest_daily_package = ",
                    latest_daily_package);
            }

        });

        req.end(manifest_str);


    });

}


function getLastUploadedDailyPackage(callback) {

    winston.info("getting last uploaded daily package");

    getLedgerManifest(function(err, manifest) {

        if (err || !manifest.latest_daily_package) {

            winston.info("first day:", moment(FIRST_DAY).format("YYYY-MM-DD"));
            callback(null, moment(FIRST_DAY).format("YYYY-MM-DD"));

        } else {

            winston.info("last uploaded daily package:", manifest.latest_daily_package);
            callback(null, manifest.latest_daily_package);

        }
    });
}

function getLedgerManifest(callback) {

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