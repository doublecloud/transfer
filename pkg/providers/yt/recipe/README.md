## YT Saurus Recipe

This recipe is either start docker container with yt-local or use predifined YT_PROXY.
For better debug there is docker-compose.yml which provide local YT with UI, so you can check out how it looks with your own eyes.

Like https://github.com/ytsaurus/ytsaurus/blob/main/yt/docker/local/run_local_cluster.sh, but automated.

To run this in tests simply:

```go
Target = yt_recipe.RecipeYtTarget("//home/cdc/test/pg2yt_e2e")
```

And this will spawn a container and create a target connection to this container.

