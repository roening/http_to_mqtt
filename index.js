var settings = {
    mqtt: {
        host: process.env.MQTT_HOST || '',
        user: process.env.MQTT_USER || '',
        password: process.env.MQTT_PASS || '',
        clientId: process.env.MQTT_CLIENT_ID || null,
    },
    keepalive: {
        topic: process.env.KEEP_ALIVE_TOPIC || 'keep_alive',
        message: process.env.KEEP_ALIVE_MESSAGE || 'keep_alive'
    },
    debug: process.env.DEBUG_MODE || false,
    auth_key: process.env.AUTH_KEY || '',
    http_port: process.env.PORT || 5000
}

var mqtt = require('mqtt');
var express = require('express');
var bodyParser = require('body-parser');
var multer = require('multer');

const commandTopic = 'command/' + settings.mqtt.user + '/1';
const responseTopic = 'response/' + settings.mqtt.user + '/1';

var app = express();

function getMqttClient() {

    var options = {
        username: settings.mqtt.user,
        password: settings.mqtt.password
    };

    if (settings.mqtt.clientId) {
        options.clientId = settings.mqtt.clientId
    }

    return mqtt.connect(settings.mqtt.host, options);
}

var mqttClient = getMqttClient();
mqttClient.subscribe(responseTopic);

app.set('port', settings.http_port);
app.use(bodyParser.json());

function logRequest(req, res, next) {
    var ip = req.headers['x-forwarded-for'] ||
        req.connection.remoteAddress;
    var message = 'Received request [' + req.originalUrl +
        '] from [' + ip + ']';

    if (settings.debug) {
        message += ' with payload [' + JSON.stringify(req.body) + ']';
    } else {
        message += '.';
    }
    console.log(message);

    next();
}

function authorizeUser(req, res, next) {
    if (settings.auth_key && req.body['key'] != settings.auth_key) {
        console.log('Request is not authorized.');
        res.sendStatus(401);
    }
    else {
        next();
    }
}

function checkSingleFileUpload(req, res, next) {
    if (req.query.single) {
        var upload = multer().single(req.query.single);

        upload(req, res, next);
    }
    else {
        next();
    }
}

function checkMessagePathQueryParameter(req, res, next) {
    if (req.query.path) {
        req.body.message = req.body[req.query.path];
    }
    next();
}

function checkTopicQueryParameter(req, res, next) {

    if (req.query.topic) {
        req.body.topic = req.query.topic;
    }

    next();
}

function ensureTopicSpecified(req, res, next) {
    if (!req.body.topic) {
        res.status(500).send('Topic not specified');
    }
    else {
        next();
    }
}

app.get('/keep_alive/', logRequest, function (req, res) {
    mqttClient.publish(settings.keepalive.topic, settings.keepalive.message);
    res.sendStatus(200);
});

const receiveMessage = new Promise((response) => {
    mqttClient.on('message', function (topic, message) {
        parsedMessage = JSON.parse(message);
        response(parsedMessage);
    });
})

app.post('/post/', logRequest, authorizeUser, checkSingleFileUpload, checkMessagePathQueryParameter, function (req, res) {
    mqttClient.publish(commandTopic, JSON.stringify(req.body));
    receiveMessage.then(message => {
        res.status(200).json(message);
    })
});

app.get('/subscribe/', logRequest, authorizeUser, function (req, res) {

    var topic = req.query.topic;

    if (!topic) {
        res.status(500).send('topic not specified');
    }
    else {
        // get a new mqttClient
        // so we dont constantly add listeners on the 'global' mqttClient
        var mqttClient = getMqttClient();

        mqttClient.on('connect', function () {
            mqttClient.subscribe(topic);
        });

        mqttClient.on('message', function (t, m) {
            if (t === topic) {
                res.write(m);
            }
        });

        req.on("close", function () {
            mqttClient.end();
        });

        req.on("end", function () {
            mqttClient.end();
        });
    }
});

app.listen(app.get('port'), function () {
    console.log('Node app is running on port', app.get('port'));
});
