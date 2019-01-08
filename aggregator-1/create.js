var pg = require('pg');
var config = require('./config.json');

pg.connect(config.dsn, function(err, client, done){
    if(err) console.error(error)
    else {
        client.query(
            "create table " + config.table + " (" +
            "       name varchar(50)," +
            "       value integer," +
            "       timestamp timestamp not null default current_timestamp)"
            , function(err, res) {
            done();
            if(err) log(err)
            else log(s, 'has been loaded')
        })
        client.end();
    }
});
