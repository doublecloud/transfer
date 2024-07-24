# Middlewares

Middlewares are common code components that are intended to be reused by multiple sources, sinks, or in general transfer pipeline.

Each middleware is a function with the following properties:
1. Its arguments are its configuration properties &mdash; the ones which configure the behaviour of this middleware;
2. Its result is a *function* which can be used *between* source and sink &mdash; that is, it *should* fulfill at least one of two properties:
    * It accepts an interface from `abstract`, making no assumptions about possible middlewares which wrap the actual object;
    * It returns an interface from `abstract`, for which the callers must not make assumptions about possible middlewares which wrap it.

The second requirement defines the main property to call a function "middleware" &mdash; it operates on abstract interfaces.
