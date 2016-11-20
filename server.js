var express = require('express'),
    path = require('path'),
    http = require('http'),
    wine = require('./routes/wines');

console.log('finished requires; creating express');
var app = express();

app.set('port', process.env.PORT || 3000);

app.get('/wines', wine.findAll);
app.get('/wines/:id', wine.findById);
app.post('/wines', wine.addWine);
app.put('/wines/:id', wine.updateWine);
app.delete('/wines/:id', wine.deleteWine);

http.createServer(app).listen(app.get('port'), function () {
    console.log("Express server listening on port " + app.get('port'));
});
