import * as cdk from 'aws-cdk-lib';
import * as events from 'aws-cdk-lib/aws-events';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import {Construct} from 'constructs';
import {PythonFunction} from '@aws-cdk/aws-lambda-python-alpha';


export class StiBusDelaysStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        const dataBucket = new s3.Bucket(this, 'data-bucket', {
            blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
            encryption: s3.BucketEncryption.S3_MANAGED,
            enforceSSL: true,
            versioned: false,
            eventBridgeEnabled: true
        });

        const actualDataDownloaderFunction = new PythonFunction(this, 'actual-data-downloader', {
            description: 'Actual data downloader',
            entry: '../application/src/actual-data-downloader',
            runtime: lambda.Runtime.PYTHON_3_9,
            bundling: {
                assetExcludes: ['.venv'],
            },
            timeout: cdk.Duration.seconds(30),
            memorySize: 3072,
            environment: {
                DATASET_URL: "https://opentransportdata.swiss/de/dataset/istdaten/permalink",
                OUTPUT_PATH: `s3://${dataBucket.bucketName}/actual-data`,
                OUTPUT_FILE_NAME: "sti_thun_bahnhof.csv"
            }
        });

        const dailyChartGeneratorFunction = new PythonFunction(this, 'daily-chart-generator', {
            description: 'Daily chart generator',
            entry: '../application/src/daily-chart-generator',
            runtime: lambda.Runtime.PYTHON_3_9,
            bundling: {
                assetExcludes: ['.venv'],
            },
            timeout: cdk.Duration.seconds(10),
            memorySize: 256,
            environment: {
                DATA_BUCKET_NAME: dataBucket.bucketName,
                MPLCONFIGDIR: "/tmp/matplotlib"
            }
        });

        const monthlyChartGeneratorFunction = new PythonFunction(this, 'monthly-chart-generator', {
            description: 'Monthly chart generator',
            entry: '../application/src/monthly-chart-generator',
            runtime: lambda.Runtime.PYTHON_3_9,
            bundling: {
                assetExcludes: ['.venv'],
            },
            timeout: cdk.Duration.seconds(90),
            memorySize: 320,
            environment: {
                DATA_BUCKET_NAME: dataBucket.bucketName,
                MPLCONFIGDIR: "/tmp/matplotlib"
            }
        });

        dataBucket.grantReadWrite(actualDataDownloaderFunction)
        dataBucket.grantReadWrite(dailyChartGeneratorFunction)
        dataBucket.grantReadWrite(monthlyChartGeneratorFunction)

        const actualDataCreatedRule = new events.Rule(this, 'rule', {
            description: "Actual data object created",
            eventPattern: {
                source: ['aws.s3'],
                detailType: [
                    'Object Created'
                ],
                detail: {
                    bucket: {
                        name: [dataBucket.bucketName]
                    },
                    object: {
                        key: [
                            {wildcard: "actual-data/*/*.csv"}
                        ]
                    }
                }
            },
        });

        actualDataCreatedRule.addTarget(new targets.LambdaFunction(dailyChartGeneratorFunction, {
            maxEventAge: cdk.Duration.hours(2),
            retryAttempts: 3
        }));
        actualDataCreatedRule.addTarget(new targets.LambdaFunction(monthlyChartGeneratorFunction, {
            maxEventAge: cdk.Duration.hours(2),
            retryAttempts: 3
        }));

        const scheduleRule = new events.Rule(this, 'ScheduleRule', {
            description: "Schedule a Lambda that downloads a report every day",
            schedule: events.Schedule.cron({
                year: "*",
                month: "*",
                day: "*",
                hour: "12",
                minute: "15",
            })
        });

        scheduleRule.addTarget(new targets.LambdaFunction(actualDataDownloaderFunction));
    }
}
