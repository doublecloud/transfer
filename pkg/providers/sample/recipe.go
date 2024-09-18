package sample

func RecipeSource() *SampleSource {
	res := &SampleSource{
		SampleType:         "",
		TableName:          "",
		MaxSampleData:      0,
		MinSleepTime:       0,
		SnapshotEventCount: 100,
	}
	res.WithDefaults()
	return res
}
