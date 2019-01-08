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
            if(res.Records.length){
                log(res.Records.length, 'records found');
                pg.connect(config.dsn, function(err, client, done) {
                    if(err) throw err;
                    client.query('begin', function(err) {
                        if(err) throw err;
                        try {
                            // insert data to redshift
                            for(var i = 0; i < res.Records.length; i++){
                                var r = JSON.parse(
                                    new Buffer(res.Records[i]['Data']
                                    || '', 'base64').toString('utf8'));
                                var t = r.time;
                                for(k in r.sensordata)
                                    client.query(
                                        "insert into " + config.table +
                                        "       (timestamp, name, value)" +
                                        "       values ($1, $2, $3)", 
                                        [t, k, r.sensordata[k]]
                                    , function(err, res) {
                                        if(err) throw err;
                                    });
                            }
                            client.query('commit', function(err) {
                                if(err) throw err;
                                log('upload completed');
                                client.end();
                            });
                        } catch (e) {
                            log(e);
                            client.query('rollback', function(err) {
                                if(err) log(err)
                                else log('transaction rollbacked');
                            });
                            client.end();
                            throw err;
                        }
                    });
                });
            }
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
