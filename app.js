const express = require('express');
var minio = require("minio");
var BodyParser = require("body-parser");
const fs = require('fs');

const app = express()

app.use(BodyParser.json({limit: "4mb"}));

const minioClient = new minio.Client({
    endPoint: 'play.minio.io',
    port: 9000,
    useSSL: true,
    accessKey: 'Q3AM3UQ867SPQQA43P2F',
    secretKey: 'zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG'
});

app.get("/upload", function(req, res) {
    const file = './outgoing/example.pdf';
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
                return res.send(presignedUrl)
            })
            // return res.send(objInfo);
        })
    });
});

// app.post("/uploadfile", multer({dest: "./uploads/"}).single("upload"), function(request, response) {
//     minioClient.fPutObject("test", request.file.originalname, request.file.path, "application/octet-stream", function(error, etag) {
//         if(error) {
//             return console.log(error);
//         }
//         response.send(request.file);
//     });
// });

app.get("/download", function(request, response) {
    // minioClient.presignedUrl('GET', 'mybucket', 'example.pdf', 24*60*60, function(err, presignedUrl) {
    //     if (err) return console.log(err)
    //     return res.send(presignedUrl)
    // })
    minioClient.getObject("mybucket", 'example.pdf', function(error, stream) {
        if(error) {
            return response.status(500).send(error);
        }
        stream.pipe(response);
    });
});

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
