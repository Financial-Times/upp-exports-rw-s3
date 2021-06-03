<!--
    Written in the format prescribed by https://github.com/Financial-Times/runbook.md.
    Any future edits should abide by this format.
-->
# upp-exports-rw-s3

Application that performs CRUD operations on content and concept files and stores them in s3

## Code

upp-exports-rw-s3

## Primary URL

https://github.com/Financial-Times/upp-exports-rw-s3

## Service Tier

Bronze

## Lifecycle Stage

Production

## Host Platform

AWS

## Architecture

This is an API for reading/writing content and concepts payloads up to S3. The api specification can be found [here](https://docs.google.com/document/d/1Ck-o0Le9cXOfm-aVjiGmOT7ZTB5W5fDTsPqGkhzfa-U/edit#heading=h.jwsnnbv7enh5)
The service is exposing following admin endpoints:

*   Healthchecks: <http://localhost:8080/__health>
*   Build Info: <http://localhost:8080/__build-info> or <http://localhost:8080/build-info>
*   GTG: <http://localhost:8080/__gtg>

## Contains Personal Data

No

## Contains Sensitive Data

No

<!-- Placeholder - remove HTML comment markers to activate
## Can Download Personal Data
Choose Yes or No

...or delete this placeholder if not applicable to this system
-->

<!-- Placeholder - remove HTML comment markers to activate
## Can Contact Individuals
Choose Yes or No

...or delete this placeholder if not applicable to this system
-->

## Failover Architecture Type

ActiveActive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The service is deployed in both Delivery clusters. The failover guide for the cluster is located here:
<https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/delivery-cluster>

## Data Recovery Process Type

NotApplicable

## Data Recovery Details

No Data recovery is needed

## Release Process Type

PartiallyAutomated

## Rollback Process Type

Manual

## Release Details

The service is deployed with Jenkins job

<!-- Placeholder - remove HTML comment markers to activate
## Heroku Pipeline Name
Enter descriptive text satisfying the following:
This is the name of the Heroku pipeline for this system. If you don't have a pipeline, this is the name of the app in Heroku. A pipeline is a group of Heroku apps that share the same codebase where each app in a pipeline represents the different stages in a continuous delivery workflow, i.e. staging, production.

...or delete this placeholder if not applicable to this system
-->

## Key Management Process Type

Manual

## Key Management Details

To access the job clients need to provide basic auth credentials to log into the k8s clusters.
To rotate credentials you need to login to a particular cluster and update varnish-auth secrets.

## Monitoring

<p><a href="https://upp-prod-delivery-eu.ft.com/__health/__pods-health?service-name=upp-exports-rw-s3" target="_blank">Prod-EU Health</a></p><p><a href="https://upp-prod-delivery-us.ft.com/__health/__pods-health?service-name=upp-exports-rw-s3" target="_blank">Prod-US Health</a><br><br></p>

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.