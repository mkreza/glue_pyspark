### Usage Guide
This repository contains a docker-compose file which will help in setting up a development environment for AWS Glue Pyspark.

Ensure the following files are available before starting:
- AWS config file `~/.aws/config`
- `.env` file for configuring paths and variables
- `.vscode` folder at the root of this repo must be copied to workspace folder.

Run the following command to get started:
```sh
docker compose up -d
```

It is highly recommended that VSCode be used for development, wherein the editor can be attached to the running container for power editing and tooling.

### Resources
- [AWS Glue Docker Guide](https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/)