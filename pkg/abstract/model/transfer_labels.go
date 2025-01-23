package model

// SystemLabel contains transfer label names which are reserved to control
// hidden experimental transfer features.
//
// Each SystemLabel value must be documented.
type SystemLabel string

// SystemLabelMemThrottle is used to enable/disable MemoryThrorrler middleware.
//
// The only known value is "on", any other value means disabled.
const SystemLabelMemThrottle = SystemLabel("dt-mem-throttle")

// SystemLabelAsyncCH is used to enable experimental asynchronous clickhouse snapshot sink
//
// The only known value is "on", any other value means disabled.
const SystemLabelAsyncCH = SystemLabel("dt-async-ch")
