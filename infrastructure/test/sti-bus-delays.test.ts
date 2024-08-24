import * as cdk from 'aws-cdk-lib';
import {Template} from 'aws-cdk-lib/assertions';
import * as StiBusDelays from '../lib/sti-bus-delays-stack';

test('Resources Created', () => {
    const app = new cdk.App();

    const stack = new StiBusDelays.StiBusDelaysStack(app, 'MyTestStack');

    const template = Template.fromStack(stack);

    template.resourceCountIs('AWS::Lambda::Function', 4);
    template.resourceCountIs('AWS::S3::Bucket', 1);
    template.resourceCountIs('Custom::S3BucketNotifications', 1);
    template.resourceCountIs('AWS::Events::Rule', 2);
});
