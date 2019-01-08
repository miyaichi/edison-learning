var aws = require('aws-sdk');
var mraa = require('mraa');

var config = require('./config.json');
aws.config.update({
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,
    region: config.region
});

var kinesis = new aws.Kinesis();
var sensors = new Array();
for(n in config.sensors)
    sensors.push({
        name: n,
        aio: new mraa.Aio(config.sensors[n])
    })

setInterval(
    function (){
        // read sensor data
        var record = {
            time: new Date(),
            sensordata: {}
        };
        for(i = 0; i < sensors.length; i++){
            var s = sensors[i].aio.read();
            if(converter[sensors[i].name]) s = converter[sensors[i].name](s);
            record.sensordata[sensors[i].name] = s;
        }

        var params = {
            Data: JSON.stringify(record),
            PartitionKey: config.partitionKey,
            StreamName: config.stream
        };
        kinesis.putRecord(params, function(err, data){
            if (err) log(err, err.stack)
            else log(record.sensordata)
        });
    },
    1000
);

function log(){
    var args = Array.prototype.slice.call(arguments);
    args.unshift((new Date()).toString());
    console.log.apply(console, args);
}

var converter = {
    "light": function(a) {
        // convert to lux                          
        return Math.round(((1023 - a) * 10 / a) * 100) / 100;
    },
    "temperature": function(a) {
        // convert to centigrade                                             
        var B = 3975;
        var r = (1023 - a) * 10000 / a;
        return Math.round(
            (1 / (Math.log(r / 10000) / B + 1 / 298.15) - 273.15)
            * 100) / 100;
    }
}
