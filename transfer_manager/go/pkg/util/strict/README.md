# `strict`: generic cast functions for conversion to strict types

This package contains generic cast functions enabling `provider`s to convert the values obtained from source databases to the strictly-typed values expected in heterogenous transfers. The main purpose of these functions is to just call the cast, providing a single-line "interface" to do that in concrete providers' type switch statements.

The functions in this package are divided into two categories: `Expected` and `Unexpected`. They work the same way in regards to the type / value conversion. They differ in just one way. The `Expected` functions return errors different from the errors returned by the `Unexpected` functions when a cast error occurs. The difference depends on the type of the incoming value being casted.
