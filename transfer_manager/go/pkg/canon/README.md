# The Native canon package

Canonization test are what the industry calls golden test. Reference files are generated against which
the output of tests can be verified to avoid unwanted divergence in the future.

This new canon package is tailored to work with the existing canonization methodology and avoid the
necessity to regenerate all canon files.


## Differences to ya make canonization
- External reference files are not created and skipped during verification
- Local reference files are not created but are verified and updated if present
- If gotest folder is present then canondata is created and verified inside this folder


## Running canon test natively

In the directory containing your test you can simply run:

```
go test -run ^TestCanon
```

This will run the TestCanon test and verify against the reference canon file.
The test will fail should the output differ.

If you want to update the reference file, or if its a new canonization test you need to pass
the necessary arguments to go test for changes to be persisted:

```
go test -run ^TestCanon -args -canonize-tests
```


## Running canon test in canon package with ya make (TODO: should be removed once OSS)

To run the tests directly in the canon package also with ya make two environment variables need to be passed:


```
ya make -DDATA_TRANSFER_DISABLE_CGO transfer_manager/go/pkg/abstract/changeitem/ -tt
```

This will run the test in the test folder and verify them against the reference canon files.

If tests need re-canonization the `CANONIZE_TESTS` env variable needs to be also passed


```
ya make -DDATA_TRANSFER_DISABLE_CGO --test-env=CANONIZE_TESTS=1 transfer_manager**/go/pkg/abstract/changeitem/ -tt
```

This will persist changes to the corresponding reference files.


## Running canon test in the old approach (TODO: should be removed once OSS)

The canon tests ca also be run similarly to before with the ya tooling:

```
ya make -DDATA_TRANSFER_DISABLE_CGO transfer_manager/go/pkg/abstract/changeitem/ -tt
```

and to regenerate by passing the -Z flag:

```
ya make -DDATA_TRANSFER_DISABLE_CGO transfer_manager/go/pkg/abstract/changeitem/ -tt -Z
```
