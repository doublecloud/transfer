# Launching MongoDB sharded cluster from code
This is just simple example how to launch your
custom cluster locally.

Note, that this code only launches MongoDB cluster
on succes, but does not stop it anyway. You should
stop it manually right now with this tricky hacks:

```ps aux | grep mongo | awk '{print $2}' | xargs -L1 kill```

And don't forget to cleanup folders that holds shard data and logs:
**IMPORTANT NOTE** be careful where you invoke this command

```rm -rf logs workspace```
