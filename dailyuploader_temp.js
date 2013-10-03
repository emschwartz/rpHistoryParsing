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

clusterAndUploadNextDay("2013-07-22");


// clusterAndUploadNextDay gets the next day of ledgers from the RippledQuerier,
// packages it, uploads it to S3, and starts clustering and uploading the next day
function clusterAndUploadNextDay(prev_day_str) {

    var this_day = moment(prev_day_str + MIDNIGHT_UTC).add("days", 1);

    winston.info("Clustering", this_day.format());

    if (this_day.format("YYYY-MM-DD") === "2013-07-26")
        return;


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


        packageDay(ledgers, function(err, daily_packages) {
            if (err) {
                winston.error("Error packaging daily ledgers:", err);
                return;
            }


            uploadToS3(this_day.format("YYYY-MM-DD"), daily_packages, function(err, day_str) {
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

    var daily_packages = [];
    var num_ledgers = ledgers.length;
    var leds_per_packet = 20000;

    var num_packets = Math.ceil(num_ledgers / leds_per_packet);

    for (var p = 0; p < num_packets.length; p++) {
        var start = p * leds_per_packet;

        var packet_txt = '';

        for (var i = 0; i < leds_per_packet; i++){
            var index = start + i;
            if (index >= num_ledgers)
                break;
            packet_txt += JSON.stringify(ledgers[index]) + '\n';
        }
        daily_packages[p] = {packet: p, txt: packet_txt};
    }
    // if (num_ledgers > leds_per_packet) {



    // } else {
    callback(null, daily_packages);

}


function uploadToS3 (day_str, daily_packages, callback) {

    // winston.info("daily_package.length", daily_package.length);

    winston.info("uploading", day_str, "in", daily_packages.length, "packages");

    async.eachSeries(daily_packages, function(daily_package_obj, async_callback){

        var daily_package = daily_package_obj.txt;
        var part = daily_package_obj.packet;

        winston.info("uploading part", part);

        var daily_package_stream = streamifier.createReadStream(daily_package);

        var upload = new MultiPartUpload({
            tmpDir: '/mnt/tmp/mpu',
            client: client,
            objectName: '/daily-packages/' + day_str + '_part' + part + '.txt',
            stream: daily_package_stream
        }, function(err, body){
            if (err) {
                async_callback(err);
                return;
            }

            winston.info("Daily package", day_str + "_part" + part, "saved to S3 at:", body.Location);

            // updateS3Manifest(day_str);
            async_callback(null);
        });

    }, function(err){
        if (err) {
            callback(err);
            return;
        }
        // updateS3Manifest(day_str);
        callback(null, day_str);
    });


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