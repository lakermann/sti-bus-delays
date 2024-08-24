#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import {StiBusDelaysStack} from '../lib/sti-bus-delays-stack';

const app = new cdk.App();
new StiBusDelaysStack(app, 'StiBusDelaysStack', {});