const amqp = require('amqplib/callback_api');
const express = require('express');
const minio = require("minio");
const BodyParser = require("body-parser");
const fs = require('fs');
const https = require('https');

const app = express()
let download_url = '';

app.use(BodyParser.json({limit: "4mb"}));

const minioClient = new minio.Client({
    endPoint: 'play.minio.io',
    port: 9000,
    useSSL: true,
    accessKey: 'Q3AM3UQ867SPQQA43P2F',
    secretKey: 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG'
});

// file upload from the local directory called Outgoing
app.get("/upload", function(req, res) {
    const file = `${__dirname}/Outgoing/example.pdf`;
    const fileStream = fs.createReadStream(file);
    fs.stat(file, function(err, stats) {
        if (err) {
            return console.log(err)
        }
        minioClient.putObject('mybucket', 'example.pdf', fileStream, stats.size, function(err, objInfo) {
            if (err) {
                return res.status(500).send(err);
            }
            minioClient.presignedUrl('GET', 'mybucket', 'example.pdf', 24*60*60, function(err, presignedUrl) {
                if (err) return console.log(err)
                download_url = presignedUrl;
                console.log('Upload Completed.')
                publish("", "mybucket_queue", new Buffer.from(download_url));
                return res.send(presignedUrl)
            })
        })
    });
});

// download file from the link to local directory called outgoing
app.get("/download", function(req, res) {
    
    minioClient.getObject("mybucket", 'example.pdf', function(error, stream) {
        if(error) {
            return response.status(500).send(error);
        }

        const dir = `${__dirname}/Incoming/example.pdf`;
        const file = fs.createWriteStream(dir);
        const request = https.get(download_url, function(response) {
            response.pipe(file);

            // after download completed close filestream
            file.on("finish", () => {
                file.close();
                console.log("Download Completed");
                res.send('Download completed')
            });
        });
    });
});


//Create new Bucket
// minioClient.makeBucket('mybucket', 'us-east-1', function(err) {
//     if (err) return console.log('Error creating bucket.', err)
//     console.log('Bucket created successfully in "us-east-1".')
// })

minioClient.bucketExists('mybucket', function(err, exists) {
    if (err) {
      return console.log(exists)
    }
    if (exists) {
        console.log('Bucket exists.')
        const server = app.listen(3000, function() {
            console.log("Listening on port %s...", server.address().port);
        });
    }
})


// if the connection is closed or fails to be established at all, we will reconnect
var amqpConn = null;
const amqp_url = 'amqp://localhost';

function start() {
  amqp.connect(amqp_url + "?heartbeat=60", function(err, conn) {
    if (err) {
      console.error("[AMQP]", err.message);
      return setTimeout(start, 1000);
    }
    conn.on("error", function(err) {
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", function() {
      console.error("[AMQP] reconnecting");
      return setTimeout(start, 1000);
    });
    console.log("[AMQP] connected");
    amqpConn = conn;
    
    whenConnected();
  });
}

function whenConnected() {
  startPublisher();
  startWorker();
}

var pubChannel = null;
var offlinePubQueue = [];

function startPublisher() {
  amqpConn.createConfirmChannel(function(err, ch) {
    if (closeOnErr(err)) return;
      ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });

    pubChannel = ch;
    while (true) {
      var m = offlinePubQueue.shift();
      if (!m) break;
      publish(m[0], m[1], m[2]);
    }
  });
}

/*** Publish Queue to RabbitMQ */
function publish(exchange, routingKey, content) {
    try {
        //publish as a producer to RabbitMQ
        
        pubChannel.publish(exchange, routingKey, content, { persistent: true },
        function(err, ok) {
            if (err) {
                console.error("[AMQP] publish", err);
                offlinePubQueue.push([exchange, routingKey, content]);
                pubChannel.connection.close();
            }
            console.log('publish completed!')
        });
    } catch (e) {
        console.error("[AMQP] publish", e.message);
        offlinePubQueue.push([exchange, routingKey, content]);
    }
}
// A worker that acks messages only if processed succesfully
function startWorker() {
  amqpConn.createChannel(function(err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function(err) {
        console.error("[AMQP] channel error", err.message);
    });

    ch.on("close", function() {
        console.log("[AMQP] channel closed");
    });

    ch.prefetch(10);
    ch.assertQueue("mybucket_queue", { durable: true }, function(err, _ok) {
        if (closeOnErr(err)) return;
        ch.consume("mybucket_queue", processMsg, { noAck: false });
        console.log("Worker is started");
    });

    function processMsg(msg) {
        work(msg, function(ok) {
            try {
                if (ok)
                    ch.ack(msg);
                else
                    ch.reject(msg, true);
            } catch (e) {
                closeOnErr(e);
            }
        });
    }
  });
}

function work(msg, cb) {
  console.log("Got msg ", msg.content.toString());
  cb(true);
}

function closeOnErr(err) {
  if (!err) return false;
  console.error("[AMQP] error", err);
  amqpConn.close();
  return true;
}

// setInterval(function() {
//     publish("", "mybucket_queue", new Buffer.from(download_url));
// }, 1000);

start();
