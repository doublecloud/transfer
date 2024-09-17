# `Bufferer`

`Bufferer` is a middleware which provides capability for asynchronous data transfer. `Bufferer` collects items coming from the source endpoint into an in-memory storage and later sends all collected items into the target endpoint *as a single batch*. `Bufferer` can dramatically optimize data transfer speed and increase throughput.

## How it works

See the code for details. A brief overview of how `Bufferer` works (in pair with `IntervalThrottler`) is provided on the picture below. The picture refers to the "old trigger" &mdash; this is how `Bufferer` worked in the past and how it *must not* be implemented. 

![Bufferer overview](./README_assets/buffering.jpg)
