package sample

func RecipeSource() *SampleSource {
	res := &SampleSource{
		SampleType:    "",
		TableName:     "",
		MaxSampleData: 0,
		MinSleepTime:  0,
	}
	res.WithDefaults()
	return res
}
