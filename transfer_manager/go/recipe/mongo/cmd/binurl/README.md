# `binurl` CLI utility
## Purpose
It's single purpose: crawling of [long page of MongoDB distributives](
https://www.mongodb.com/download-center/community/releases/archive)
in order to make list of interesting ones

## Build
Fast variant: ```go build .```

Canonical variant: ```ya make -DDATA_TRANSFER_DISABLE_CGO .```

## Usage
### Find MacOS binaries with major patch per version
```./binurl -os macos -arch x86_64 -patch-collapse```
### Find linux binaries (distribution ubuntu) with major patch per version
```./binurl -os linux -arch x86_64 -tag-contains "ubuntu" -patch-collapse -max-distr-collapse```

## Auxiliary
`ubuntu_links.txt` contains mostly `ubuntu1604` version that
can be used in Arcadia Distbuild which uses `ubuntu1604`.

