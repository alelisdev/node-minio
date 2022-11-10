const express = require('express');
var multer = require("multer");
var minio = require("minio");
var BodyParser = require("body-parser");

const app = express()

app.use(BodyParser.json({limit: "4mb"}));

const minioClient = new minio.Client({
    endPoint: 'play.minio.io',
    port: 9000,
    useSSL: true,
    accessKey: 'Q3AM3UQ867SPQQA43P2F',
    secretKey: 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG'
});

app.post("/upload", multer({storage: multer.memoryStorage()}).single("upload"), function(request, response) {
    minioClient.putObject("test", request.file.originalname, request.file.buffer, function(error, etag) {
        if(error) {
            return console.log(error);
        }
        response.send(request.file);
    });
});

app.post("/uploadfile", multer({dest: "./uploads/"}).single("upload"), function(request, response) {
    minioClient.fPutObject("test", request.file.originalname, request.file.path, "application/octet-stream", function(error, etag) {
        if(error) {
            return console.log(error);
        }
        response.send(request.file);
    });
});

app.get("/download", function(request, response) {
    minioClient.getObject("test", request.query.filename, function(error, stream) {
        if(error) {
            return response.status(500).send(error);
        }
        stream.pipe(response);
    });
});

minioClient.bucketExists("test", function(error) {
    if(error) {
        return console.log(error);
    }
    const server = app.listen(3000, function() {
        console.log("Listening on port %s...", server.address().port);
    });
});