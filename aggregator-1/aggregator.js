var aws = require('aws-sdk');
var du = require('date-utils');
var fs = require('fs');
var mktemp = require('mktemp');
var pg = require('pg');
var zlib = require('zlib');

var config = require('./config.json');
aws.config.update({
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    region: config.region
});

var kinesis = new aws.Kinesis();
kinesis.describeStream({StreamName: config.stream}, function(err, res){
    if(err) log(err)
    else
        for(var i = 0; i < res.StreamDescription.Shards.length; i++){
            var shardId = res.StreamDescription.Shards[i].ShardId;
            var params = {
                ShardId: shardId,
                ShardIteratorType: 'LATEST',
                StreamName: config.stream
            };
            kinesis.getShardIterator(params, function(err, res){
                if(err) log(err)
                else getRecords(kinesis, shardId, res.ShardIterator);
            });
        }
});

function getRecords(kinesis, shardId, shardIterator){
    try {
        kinesis.getRecords({
            ShardIterator: shardIterator,
            Limit: 10000
        }, function(err, res){
            if(err) throw err;
            if(res.Records.length)
                mktemp.createFile('./tmp/XXXXX.csv', function(err, path){
                    if(err) throw err;
                    log(res.Records.length, 'records found');
                    // write to file
                    var ws = fs.createWriteStream(path);
                    for(var i = 0; i < res.Records.length; i++){
                        var r = JSON.parse(new Buffer(res.Records[i]['Data']
                            || '', 'base64').toString('utf8'));
                        var t = r.time;
                        for(k in r.sensordata)
                            ws.write([t, k, r.sensordata[k]].join(',') + '\n');
                    }
                    ws.end();
                    // generate key
                    var key = shardId + '-' +
                        (new Date()).toFormat("YYYYMMDDHH24MISS") + '.gz';
                    // upload to S3
                    var rs = fs.createReadStream(path).pipe(zlib.createGzip());
                    var s3 = new aws.S3({
                        params: {Bucket: config.bucket, Key: key}});
                    s3.upload({Body: rs}, function(err, data){
                        if(err) throw err;
                        log(data.Location, 'has been uploaded');
                        // remove temp file
                        fs.unlink(path);
                        // copy to Redshift
                        try {
                            pg.connect(config.dsn, function(err, client, done){
                                if(err) throw err;
                                var s = 's3://' + config.bucket + '/' + key;
                                var c = 'aws_access_key_id=' +
                                    config.accessKeyId + ';' +
                                    'aws_secret_access_key=' +
                                    config.secretAccessKey;
                                client.query(
                                    "COPY " + config.table +
                                    " FROM \'" + s + "\'" +
                                    " CREDENTIALS \'" + c + "\'" +
                                    " DELIMITER \',\' CSV GZIP" +
                                    " TIMEFORMAT \'auto\'", function(err, res) {
                                    done();
                                    if(err) log(err)
                                    else log(s, 'has been loaded')
                                });
                            });
                        } catch(e){
                            // remove s3 object
                            s3.deleteObject(function(err, data) {
                                if(err) log(err)
                            });
                            throw err;
                        }
                    });
                })
            else log('record not found')
            setTimeout(function(){
                getRecords(kinesis, shardId, res.NextShardIterator);
            }, 60000);
        });
    } catch(e){
        log(e);
        // retry
        setTimeout(function(){
            getRecords(kinesis, shardId, res.ShardIterator);
        }, 60000);
    }
}

function log(){
    var args = Array.prototype.slice.call(arguments);
    args.unshift((new Date()).toString());
    console.log.apply(console, args);
}
