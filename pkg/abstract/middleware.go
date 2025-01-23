package abstract

type Middleware func(sinker Sinker) Sinker

type AsyncMiddleware func(sink AsyncSink) AsyncSink

type Asyncer func(sinker Sinker) AsyncSink

// ComposeAsyncMiddleware builds a pipeline of AsyncMiddlewares.
// Arguments order should be the same as the desired data flow.
// ComposeAsyncMiddleware(A, B, C)(sink) call is equivalent to A(B(C(sink))).
func ComposeAsyncMiddleware(mw ...AsyncMiddleware) AsyncMiddleware {
	return func(sink AsyncSink) AsyncSink {
		for i := len(mw) - 1; i >= 0; i-- {
			sink = mw[i](sink)
		}
		return sink
	}
}
