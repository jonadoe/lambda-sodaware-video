'use strict';

const {Lambda} = require("@aws-sdk/client-lambda");
const {MediaConvert} = require("@aws-sdk/client-mediaconvert");
const SodaBaseConfigStore = require('@sodatechag/npm-base-config');
const SodaCache = require('@sodatechag/npm-cache');
const SodaDb = require('@sodatechag/npm-db');
const SodaConfig = require('@sodatechag/npm-config');
const { S3Client, HeadObjectCommand, S3 } = require("@aws-sdk/client-s3");


module.exports.createPreviews = async (event) => {
    let projectId = Number(event?.projectId);
    let assetId = Number(event?.assetId);

    const videoSizes = {
        'sd': 'public',
        'hd': 'cprev'
    };

    const dbFunctionName = 'sodaware-video-prod-createPreviews';


    try {
        let cache = SodaCache.getInstance(projectId, [ process.env.memcache ]);
        let baseConfigStore = SodaBaseConfigStore.getInstance(projectId, cache);
        let db = SodaDb.getInstance(projectId, await baseConfigStore.get('db') );
        let config = SodaConfig.getInstance(projectId, db, cache);

        let videoDownloadSizes = await config.get('VIDEO_SIZES');

        if( !("PREVIEW" in videoDownloadSizes) ) {
            throw new Error('Invalid download size');
        }

        if( !(assetId > 0) ) {
            throw new Error('Invalid assetId');
        }


        const assetData = await getAssetData(db, assetId);
        if( assetData?.asset_type != 2) {
            throw new Error('Asset is not a video');
        }

        const bucket = await config.get('AMAZON_S3_BUCKET_PICS');

        const dotPos = assetData.picname.lastIndexOf('.');
        const assetFileNameWithoutExtension = assetData.picname.substring(0, (dotPos > 0) ? dotPos : assetData.picname.length );

        for( const size in videoSizes ) {
            if (!(size in videoDownloadSizes.PREVIEW) || !videoDownloadSizes.PREVIEW[size]) {
                continue;
            }

            const s3KeyWithoutExtension = assetData.pcode + '/' + videoSizes[size] + '/' + assetFileNameWithoutExtension;
            const destinationUrl = "s3://" + bucket + "/" + s3KeyWithoutExtension;
            const destinationUrlExtension = "mp4";

            const preset = videoDownloadSizes?.PREVIEW[size];

            const location = await getMediaFileLocation(projectId, assetId);

            console.log('MEDIA FILE LOCATION IS: ' + location);

            let jobId = await convertVideoFile(location, preset, destinationUrl, destinationUrlExtension, projectId, null, assetId, size, dbFunctionName, true);

            console.log('Final file will be: ' + destinationUrl + '.' + destinationUrlExtension);
        }

        return {
            "status": "success"
        };

    } catch (error) {
        console.error('EEEEEERROR: ' + error.message);
        console.log(error);

        return {
            "status": "error",
            "message": error.message
        };
    }

}

module.exports.createDownload = async (event) => {

    const projectId = Number(event?.projectId);
    const statusToken = event?.statusToken;
    const userId = Number(event?.userId);
    const assetId = Number(event?.assetId);
    const size = event?.size.toLowerCase();

    const dbFunctionName = 'sodaware-video-prod-createDownload';
    const sizesToCopy = ['original', 'wprev'];

    try {

        let cache = SodaCache.getInstance(projectId, [ process.env.memcache ]);
        let baseConfigStore = SodaBaseConfigStore.getInstance(projectId, cache);
        let db = SodaDb.getInstance(projectId, await baseConfigStore.get('db') );
        let config = SodaConfig.getInstance(projectId, db, cache);

        let videoDownloadSizes = await config.get('VIDEO_SIZES');

        if( !sizesToCopy.includes(size) && (!("DOWNLOAD" in videoDownloadSizes) || !(size in videoDownloadSizes.DOWNLOAD) || !videoDownloadSizes.DOWNLOAD[size]) ) {
            throw new Error('Invalid download size');
        }

        if( !(assetId > 0) ) {
            throw new Error('Invalid assetId');
        }

        const assetData = await getAssetData(db, assetId);
        if( assetData?.asset_type != 2) {
            throw new Error('Asset is not a video');
        }

        const bucket = await config.get('AMAZON_S3_BUCKET_PICS');
        const s3KeyWithoutExtension = "downloads/video/"+ size +"/"+ assetId;
        const destinationUrl = "s3://"+ bucket +"/"+ s3KeyWithoutExtension;
        const destinationUrlExtension = "mp4";
        const s3Key = s3KeyWithoutExtension +'.'+ destinationUrlExtension;
        const finalFile = destinationUrl +'.'+ destinationUrlExtension;

        if( await s3FileExistsAndIsFreshEnough(bucket, s3Key) ) {

            try {
                await db.query("INSERT INTO Lambda_Status SET ?", {
                    function_name: dbFunctionName,
                    token: statusToken,
                    time_start: db.raw('NOW()'),
                    time_heartbeat: db.raw('NOW()'),
                    time_end: db.raw('NOW()'),
                    'status': 'finished',
                    data: finalFile
                });
            }
            catch(error) {
                throw new Error('Duplicate statusToken');
            }

            return {
                "status": "success",
                "statusToken": statusToken
            };
        }

        if( sizesToCopy.includes(size) ) {

            let location = null;

            if( size == 'original' ) {
                location = await getMediaFileLocation(projectId, assetId);
            }
            else if( size == 'wprev' ) {
                const dotPos = assetData.picname.lastIndexOf('.');
                const assetFileNameWithoutExtension = assetData.picname.substring(0, (dotPos > 0) ? dotPos : assetData.picname.length );

                location = 's3://'+ bucket +'/'+ assetData.pcode +'/public/'+ assetFileNameWithoutExtension +'.mp4';
            }
            else {
                throw new Error('Unknown size');
            }

            await copyS3File(location, bucket, s3Key);

            try {
                await db.query("INSERT INTO Lambda_Status SET ?", {
                    function_name: dbFunctionName,
                    token: statusToken,
                    time_start: db.raw('NOW()'),
                    time_heartbeat: db.raw('NOW()'),
                    time_end: db.raw('NOW()'),
                    'status': 'finished',
                    data: location
                } );
            }
            catch(error) {
                throw new Error('Duplicate statusToken');
            }

            return {
                "status": "success",
                "statusToken": statusToken
            };
        }

        const location = await getMediaFileLocation(projectId, assetId);

        const preset = videoDownloadSizes?.DOWNLOAD[ size ];

        let jobId = await convertVideoFile(location, preset, destinationUrl, destinationUrlExtension, projectId, statusToken, assetId, size, dbFunctionName, false);



        await db.query("INSERT INTO Lambda_Status SET ?", {
            function_name: dbFunctionName,
            token: statusToken,
            time_start: db.raw('NOW()'),
            time_heartbeat: db.raw('NOW()'),
            'status': 'pending',
            data: jobId
        } );

        console.log("Job ID is: "+ jobId);
        console.log('Final file will be: '+ finalFile);

        return {
            "status": "success",
            "statusToken": statusToken
        };

    } catch (error) {
        console.error('EEEEEERROR: ' + error.message);
        console.log(error);

        return {
            "status": "error",
            "message": error.message
        };
    }
};


async function callLambda(functionName, payload) {
    const client = new Lambda({region: "eu-central-1"});

    const response = await client.invoke({
        FunctionName: functionName,
        Payload: JSON.stringify(payload)
    });

    return JSON.parse(Buffer.from(response.Payload).toString());
}

async function getMediaFileLocation(projectId, assetId) {
    let output = await callLambda("sodaware-functions-prod-getOriginalHiresFile", {
        "projectId": Number(projectId),
        "picId": assetId
    });

    if (output?.status != 'success' || !output?.location) {
        throw new Error('Could not get Corelog file location');
    }

    return output.location;
}


async function convertVideoFile(videoFileUrl, Preset, destinationUrl, destinationUrlExtension, projectId, statusToken, assetId, size, dbFunctionName, publicAccess) {

    const cannedAcl = publicAccess ? "PUBLIC_READ" : "BUCKET_OWNER_FULL_CONTROL";

    const client = new MediaConvert({endpoint: 'https://yk2lhke4b.mediaconvert.eu-central-1.amazonaws.com'});

    const params = {
        "Queue": "arn:aws:mediaconvert:eu-central-1:867457575026:queues/Default",
        "UserMetadata": {
            "projectId": projectId,
            "statusToken": statusToken,
            "assetId": assetId,
            "size": size,
            "dbFunctionName": dbFunctionName
        },
        "Role": "arn:aws:iam::867457575026:role/service-role/MediaConvert_Default_Role",
        "Settings": {
            "TimecodeConfig": {
                "Source": "ZEROBASED"
            },
            "OutputGroups": [
                {
                    "Name": "File Group",
                    "Outputs": [
                        {
                            "Preset": Preset,
                            "Extension": destinationUrlExtension
                        }
                    ],
                    "OutputGroupSettings": {
                        "Type": "FILE_GROUP_SETTINGS",
                        "FileGroupSettings": {
                            "Destination": destinationUrl,
                            "DestinationSettings": {
                                "S3Settings": {
                                    "AccessControl": {
                                        "CannedAcl": cannedAcl
                                    }
                                }
                            }
                        }
                    }
                }
            ],
            "Inputs": [
                {
                    "AudioSelectors": {
                        "Audio Selector 1": {
                            "DefaultSelection": "DEFAULT"
                        }
                    },
                    "VideoSelector": {},
                    "TimecodeSource": "ZEROBASED",
                    "FileInput": videoFileUrl
                }
            ]
        },
        "AccelerationSettings": {
            "Mode": "DISABLED"
        },
        "StatusUpdateInterval": "SECONDS_15",
        "Priority": 0
    };

    const response = await client.createJob(params);

    return response?.Job?.Id;
}

async function getAssetData(db, assetId) {
    let sql = "SELECT p.picname, s.pcode, p.asset_type \
                FROM Picture p \
                JOIN Picture_Supplier s ON s.user_id=p.user_id \
               WHERE p.pic_id=?";
    let result = await db.query(sql, [
        assetId
    ]);

    if( !result || !result[0] ) {
        throw new Error('Could not get asset informations');
    }

    return result[0];
}

async function s3FileExistsAndIsFreshEnough(bucket, key) {
    const s3Client = new S3Client({
      region: "eu-west-1",
    });

    try {
        const response = await s3Client.send( new HeadObjectCommand({
            'Bucket': bucket,
            'Key': key
        }) );

        const expirationDate = new Date(response?.Expiration?.match('"([^"]+)GMT"')[0]);
        const expiresIn = (expirationDate.valueOf() - Date.now()) / 1000;

        console.log("Expires in seconds: "+ expiresIn);

        if(expiresIn > 10800 ) { // 3 hours
            console.log("FILE is fresh enough!");
            return true;
        }

    } catch (err) {
        // console.log("S3 ERROR: "+ err.message);
        // ignore
    }

    return false;
}

async function copyS3File(copySource, bucket, key) {

    if(copySource.substring(0,5) == 's3://') {
        copySource = copySource.substring(5);
    }

    try {
        const s3Client = new S3({
          region: "eu-west-1",
        });

        await s3Client.copyObject({
            'Bucket': bucket,
            'Key': key,
            'CopySource': copySource,
            'ACL': "public-read"
        });

        console.log("copied S3 object");

        return true;

    } catch (err) {
        console.log("Could not copy S3 object: "+ err.message);
        // ignore
    }
    return false;
}