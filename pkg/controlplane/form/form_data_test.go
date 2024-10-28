package form

import (
	"testing"

	prototest "github.com/doublecloud/transfer/proto/test"
	"github.com/stretchr/testify/require"
)

func TestClearHiddenFields(t *testing.T) {
	testProto := prototest.VisibilityTest{
		VisibleField: "data1",
		HiddenField:  "data2",
		TestOneOf: &prototest.VisibilityTest_OneOfDummy_1{
			OneOfDummy_1: &prototest.VisibilityTestDummy1{
				VisibleField: "data1",
				HiddenField:  "data2",
			},
		},
		Dummy_1: &prototest.VisibilityTestDummy1{
			VisibleField: "data1",
			HiddenField:  "data2",
		},
		Dummy_2: &prototest.VisibilityTestDummy2{
			VisibleField: "data1",
			HiddenField:  "data2",
		},
	}

	err := ClearFields(testProto.ProtoReflect(), true, false, false, false)
	require.NoError(t, err)

	require.Equal(t, "data1", testProto.VisibleField)
	require.Equal(t, "", testProto.HiddenField)

	switch oneOfDummy := testProto.TestOneOf.(type) {
	case *prototest.VisibilityTest_OneOfDummy_1:
		require.Equal(t, "data1", oneOfDummy.OneOfDummy_1.VisibleField)
		require.Equal(t, "", oneOfDummy.OneOfDummy_1.HiddenField)
	case *prototest.VisibilityTest_OneOfDummy_2:
		require.Equal(t, "data1", oneOfDummy.OneOfDummy_2.VisibleField)
		require.Equal(t, "", oneOfDummy.OneOfDummy_2.HiddenField)
	}

	require.Equal(t, "data1", testProto.Dummy_1.VisibleField)
	require.Equal(t, "", testProto.Dummy_1.HiddenField)

	require.Equal(t, "data1", testProto.Dummy_2.VisibleField)
	require.Equal(t, "", testProto.Dummy_2.HiddenField)
}
