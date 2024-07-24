## Using OpenSearch via ElasticSearch official client

Official ElasticSearch client checks if server is truly 'ElasticSearch' by two parts:

- Before every request - it can be turned-off by parameter 'useResponseCheckOnly' in config
  - so, we set UseResponseCheckOnly:true
- After first request - and sets private field in client: 'productCheckSuccess' to 'true' - in case of success
  - so, we call 'setProductCheckSuccess' function to set it into true

As result, we can work with OpenSearch via ElasticSearch official client.

If new version of ElasticSearch client won't contain 'productCheckSuccess' field - test 'TestSetProductCheckSuccess' will show it.
