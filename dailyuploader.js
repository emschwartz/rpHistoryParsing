var winston = require('winston'),
    ripple = require('ripple-lib'),
    moment = require('moment'),
    _ = require('lodash'),
    async = require('async'),
    knox = require('knox'),
    streamifier = require('streamifier'),
    MultiPartUpload = require('knox-mpu'),
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






// startClusteringDays gets the last uploaded daily package and starts clustering the next one
function startClusteringDays() {

    getLastUploadedDailyPackage(function(err, prev_day_str) {
        if (err) {
            winston.error("Error geting last daily package:", err);
            return;
        }

        clusterAndUploadNextDay(prev_day_str);

    });

}


// clusterAndUploadNextDay gets the next day of ledgers from the RippledQuerier,
// packages it, uploads it to S3, and starts clustering and uploading the next day
function clusterAndUploadNextDay(prev_day_str) {

    var this_day = moment(prev_day_str + MIDNIGHT_UTC).add("days", 1);

    winston.info("Clustering", this_day.format());


    getNextDay(this_day, function(err, ledgers) {
        if (err) {
            winston.error("Error geting daily ledgers:", err);
            return;
        }

        // wait until next day is full
        var last_close_time = moment(ledgers[ledgers.length-1].close_time_human);
        if (last_close_time.diff(this_day) < 86400000) {
            setTimeout(function(){
                winston.info("Got incomplete day for", this_day.format("YYYY-MM-DD"), "waiting", 86400000 - last_close_time.diff(this_day), "ms");
                clusterAndUploadNextDay(prev_day_str);
            }, 86400000 - last_close_time.diff(this_day));
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


            uploadToS3(this_day.format("YYYY-MM-DD"), daily_package, function(err, day_str) {
                if (err) {
                    winston.error("Error uploading daily ledgers:", err);
                    return;
                }

                setImmediate(function() {
                    clusterAndUploadNextDay(day_str);
                });

            });


        });

    });
}

// getNextDay gets a day's worth of ledgers from the RippledQuerier
function getNextDay(start_day, callback) {

    rq.getLedgersForTimeRange(moment(start_day), moment(start_day).add('days', 1).subtract('seconds', 20), function(err, ledgers) {
        if (err) {
            winston.error("Error getting day of ledgers", err);
            callback(err);
            return;
        }

        // winston.info("getNextDay got this many ledgers:", ledgers.length);

        callback(null, ledgers);

    });
}

// packageDay aggregates ledgers into a string where each ledger is separated by a newline
function packageDay(ledgers, callback) {

    // var num_ledgers = ledgers.length;
    // var leds_per_packet = 10000;

    // if (num_ledgers > leds_per_packet) {



    // } else {

    var daily_txt = '';

    for (var i = 0, len = ledgers.length; i < len; i++){
        daily_txt += JSON.stringify(ledgers[i]) + '\n';
    }

    callback(null, daily_txt);

}

// uploadToS3 uploads a daily ledger package to S3
// function uploadToS3(day_str, daily_package, callback) {

//     winston.info("uploading daily package to s3:", day_str);

//     var req = client.put('/daily-packages/' + day_str + '.txt', {
//         'Content-Length': daily_package.length,
//         'Content-Type': 'text/plain'
//     });

//     req.on('error', function(err) {
//         winston.error("Error uploading daily package", day_str, "to S3", err, "trying again in 1 sec");
//         setTimeout(function() {
//             uploadToS3(day_str, daily_package, callback);
//         }, 1000);
//     });

//     req.on('response', function(res) {

//         res.on('error', function(err) {
//             winston.error("Error uploading daily package", day_str, "to S3", err, "trying again in 1 sec");
//             setTimeout(function() {
//                 uploadToS3(day_str, daily_package, callback);
//             }, 1000);
//         });

//         if (200 === res.statusCode) {
//             winston.info("Daily package", day_str, "saved to S3 at:", req.url);
//             updateS3Manifest(day_str);
//             if (callback)
//                 callback(null, day_str);
//         }

//     });

//     req.end(daily_package);

// }


function uploadToS3 (day_str, daily_package, callback) {

    // winston.info("daily_package.length", daily_package.length);

    var daily_package_stream = streamifier.createReadStream(daily_package);

    var upload = new MultiPartUpload({
        tmpDir: '/mnt/tmp/mpu',
        client: client,
        objectName: '/daily-packages/' + day_str + '.txt',
        stream: daily_package_stream
    }, function(err, body){
        if (err) {
            winston.error("Error uploading daily package", day_str, "trying again", err);
            setImmediate(function(){
                uploadToS3(day_str, daily_package, callback);
            });
            return;
        }

        winston.info("Daily package", day_str, "saved to S3 at:", body.Location);

        updateS3Manifest(day_str);

        callback(null, day_str);

    }
    );

    // upload.on("completed", function(res){
    //     winston.info("Daily package", day_str, "saved to S3 at:", body.Location);

    //     updateS3Manifest(day_str);

    //     callback(null, day_str);
    // });

    // upload.on("error", function(err){
    //     winston.error(err);
    // });

}


// updateS3Manifest updates the daily package manifest to record the last day uploaded
function updateS3Manifest(latest_daily_package) {

    getLedgerManifest(function(manifest) {

        if (!manifest)
            manifest = {};

        manifest.latest_daily_package = latest_daily_package;

        var manifest_str = JSON.stringify(manifest);

        var req = client.put('/meta/daily-ledger-manifest.json', {
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

            // winston.info("first day:", moment(FIRST_DAY).format("YYYY-MM-DD"));
            callback(null, moment(FIRST_DAY).format("YYYY-MM-DD"));

        } else {

            // winston.info("last uploaded daily package:", manifest.latest_daily_package);
            callback(null, manifest.latest_daily_package);

        }
    });
}

function getLedgerManifest(callback) {

    winston.info("getting ledger manifest");

    var req = client.get('/meta/daily-ledger-manifest.json');

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