# charcot-fulfillment Developer Guide

### Install
- Follow install steps from the [main README](../../README.md)
- 
### Local Startup

There are two modes of startup, as a Docker container or run from IntelliJ. In local environment the context used will/should always be `local`. This means that for accessing resources in AWS, the configured AWS ODP profile will be the one used to access the various resources.

#### Docker
Use the [charcot-fulfillment.sh](../../script/charcot-fulfillment.sh) shell script to startup. You can specify any of the SST stages available and pass it as an argument. This simply means that the underlying AWS resources (DynamoDB tables, S3 buckets, Lambda's, etc.) associated with that stage will be the ones used, but in a _local_ context. 

##### Useful Docker Commands


#### IntelliJ
Create a run configuration like the below.