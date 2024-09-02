package canon

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/library/go/test/canon"
	"github.com/doublecloud/transfer/library/go/test/yatest"
	"github.com/doublecloud/transfer/transfer_manager/go/pkg/util/jsonx"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"github.com/wI2L/jsondiff"
)

var (
	update               = flag.Bool("canonize-tests", false, "Generate/Update canonization file")
	IsRunningUnderGotest bool
	canonDir             = "canondata"
	canonResult          = "result.json"
	gotestDir            = "gotest"
	mu                   sync.Mutex
)

type canonObject struct {
	TestName string      `json:"test_name"`
	Data     interface{} `json:"data"`
}

type fileObject struct {
	Local    *bool    `json:"local,omitempty"`
	URI      string   `json:"uri"`
	DiffTool []string `json:"diff_tool,omitempty"`
}

type Option interface {
	isOption()
}

type diffToolOption struct {
	diffTool []string
}

func (diffToolOption) isOption() {}

func WithDiffTool(diffTool ...string) Option {
	return diffToolOption{diffTool: diffTool}
}

type isLocalOption struct {
	isLocal bool
}

func (isLocalOption) isOption() {}

func WithLocal(isLocal bool) Option {
	return isLocalOption{isLocal: isLocal}
}

func SaveJSON(t *testing.T, item interface{}) {
	if yatest.HasYaTestContext() {
		canon.SaveJSON(t, item)
		return
	}
	var canonData canonObject
	canonData.TestName = t.Name()
	canonData.Data = item

	testName := strings.Replace(t.Name(), "/", ".", -1)

	tmpCanonDir := t.TempDir()

	pathToCanonFile := filepath.Join(tmpCanonDir, testName)

	if _, err := os.Stat(pathToCanonFile); err == nil || os.IsExist(err) {
		t.Fatalf("canon file already exists. Probably you are trying to canonize data several times in a test")
	}

	jsonData, err := json.Marshal(canonData)
	if err != nil {
		t.Fatalf("invalid json: %v", err)
	}

	if err := os.Mkdir(tmpCanonDir, 0o777); err != nil && !os.IsExist(err) {
		t.Fatalf("failed to create canon directory: %v", err)
	}

	if err = os.WriteFile(pathToCanonFile, jsonData, 0o666); err != nil {
		t.Fatalf("failed to write canon data to file: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if os.Getenv("CANONIZE_TESTS") != "" {
		// In order to set this in ya make currently
		*update = true
	}

	referenceFile := referenceCanonFile(t)
	if referenceFile == "" && !*update {
		currentFilePath, err := getCallingTestFile()
		if err != nil {
			t.Fatalf("failed to find reference file filepath: %v", err)
		}

		t.Fatalf("Missing reference canon file for comparison with: %s->%s", currentFilePath, testName)
	}
	compare(t, referenceFile, pathToCanonFile)
}

func readActualContent(t *testing.T, path string) interface{} {
	currentFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read current canon file: %v", err)
	}

	return string(currentFile)
}

func FindRoot(repoPath string) (string, error) {
	isRoot := func(arcPath string) bool {
		hasMapping := false
		hasGoMod := false
		hasGoSum := false

		if _, err := os.Stat(filepath.Join(arcPath, ".arcadia.root")); err == nil {
			return true
		}

		if _, err := os.Stat(filepath.Join(arcPath, ".mapping.json")); err == nil {
			hasMapping = true
		}
		if _, err := os.Stat(filepath.Join(arcPath, "go.mod")); err == nil {
			hasGoMod = true
		}
		if _, err := os.Stat(filepath.Join(arcPath, "go.sum")); err == nil {
			hasGoSum = true
		}
		return hasMapping && hasGoMod && hasGoSum
	}

	repoPath, err := filepath.Abs(repoPath)
	if err != nil {
		return "", err
	}

	current := filepath.Clean(repoPath)
	for {
		if isRoot(current) {
			return current, nil
		}

		next := filepath.Dir(current)
		if next == current {
			return "", xerrors.New("can't find repo root")
		}

		current = next
	}
}

func referenceCanonFile(t *testing.T) string {
	currentFilePath, err := getCallingTestFile()
	if err != nil {
		t.Fatalf("failed to find reference file filepath: %v", err)
	}
	rootPath, _ := FindRoot(currentFilePath)
	dir := filepath.Dir(currentFilePath)
	dir = strings.ReplaceAll(dir, "/-S/", "")
	if rootPath != "" {
		dir = strings.ReplaceAll(dir, rootPath+"/", "")
	}
	absolutePath := yatest.SourcePath(filepath.Join(dir, canonDir, canonResult))

	_, err = os.Stat(absolutePath)
	if os.IsNotExist(err) {
		absolutePath := yatest.SourcePath(filepath.Join(dir, gotestDir, canonDir, canonResult))
		_, err := os.Stat(absolutePath)
		if os.IsNotExist(err) {
			return ""
		} else {
			require.NoError(t, err)
			return absolutePath
		}
	} else {
		require.NoError(t, err)
		return absolutePath
	}
}

func referencePathAndKey(t *testing.T) (dirPath string, testKey string) {
	currentFilePath, err := getCallingTestFile()
	if err != nil {
		t.Fatalf("failed to find reference file filepath: %v", err)
	}

	dir := filepath.Dir(currentFilePath)
	absolutePath := filepath.Join(dir, gotestDir)
	_, err = os.Stat(absolutePath)
	if os.IsNotExist(err) {
		dirPath = filepath.Join(dir, canonDir)
		_, dirName := filepath.Split(dir)
		testKey = fmt.Sprintf("%s.%s", dirName, dirName)
		return dirPath, testKey
	} else {
		dirPath = filepath.Join(dir, gotestDir, canonDir)
		testKey = fmt.Sprintf("%s.%s", gotestDir, gotestDir)
		return dirPath, testKey
	}
}

func compare(t *testing.T, referenceFile string, currentCanonFile string) {
	testName, data := getNameAndData(t, currentCanonFile)
	var referenceCanonData map[string]interface{}

	if referenceFile != "" {
		referenceCanonFile, err := os.ReadFile(referenceFile)
		if err != nil {
			t.Fatalf("Failed to read existing canon file: %v", err)
		}

		err = jsonx.Unmarshal(referenceCanonFile, &referenceCanonData)
		if err != nil {
			t.Fatalf("Failed to unmarshal existing canon file: %v", err)
		}

		missing := true
		var toAddKey string
		for key, value := range referenceCanonData {
			parts := strings.Split(key, ".")
			require.GreaterOrEqual(t, len(parts), 1)
			tName := parts[len(parts)-1]

			toAddKey = fmt.Sprintf("%s.%s.%s", parts[0], parts[0], testName)
			if tName == testName {
				missing = false
				if isExternalFile(value) {
					// skip externally saved canon files
					continue
				}

				var skipJSONDiff []string
				if containsLocalFiles(value) {
					skipJSONDiff = compareLocalAndSkipInJSON(t, value, data)
				}

				diff, err := jsondiff.Compare(value, data, jsondiff.Ignores(skipJSONDiff...))
				require.NoError(t, err)
				if !*update {
					if len(diff) != 0 {
						t.Errorf("Differences found with reference file %s vs %s:\n%s", referenceFile, currentCanonFile, prettyPrintDiff(diff, testName))
					} else {
						t.Logf("Test %s passed with no differences", testName)
					}
				}
				currentData, err := toUpdateReferenceObject(referenceCanonData[key], data, skipJSONDiff)
				if err != nil {
					t.Fatalf("Failed to compute what should be updated in reference file: %v", err)
				}

				referenceCanonData[key] = currentData
				break
			}
		}
		if missing {
			if !*update {
				t.Fatalf("Reference output for test %s is missing from reference file, file might need regeneration", testName)
			}
			referenceCanonData[toAddKey] = data
		}
	} else {
		canonDir, testKey := referencePathAndKey(t)
		if err := os.Mkdir(canonDir, 0o777); err != nil && !os.IsExist(err) {
			t.Fatalf("failed to create canon directory: %v", err)
		}
		referenceCanonData = make(map[string]interface{})
		referenceCanonData[fmt.Sprintf("%s.%s", testKey, testName)] = data
		referenceFile = filepath.Join(canonDir, canonResult)
	}

	updateReference(t, referenceFile, referenceCanonData)
}

func getNameAndData(t *testing.T, currentCanonFile string) (string, interface{}) {
	var currentCanonData map[string]interface{}

	currentFile, err := os.ReadFile(currentCanonFile)
	if err != nil {
		t.Fatalf("Failed to read current canon file: %v", err)
	}

	err = jsonx.Unmarshal(currentFile, &currentCanonData)
	if err != nil {
		t.Fatalf("Failed to unmarshal current canon file: %v", err)
	}

	testName, ok := currentCanonData["test_name"].(string)
	if !ok {
		t.Fatalf("Missing or invalid test_name field in current canon file")
	}

	data, ok := currentCanonData["data"]
	if !ok {
		t.Fatalf("Missing or invalid data field in current canon file")
	}

	dataObject, ok := data.(map[string]interface{})
	if ok {
		uri, ok := dataObject["uri"]
		if ok {
			uriStr, ok := uri.(string)
			if ok {
				data = readActualContent(t, strings.TrimPrefix(uriStr, "file://"))
			}
		}
	}
	return testName, data
}

func compareLocalAndSkipInJSON(t *testing.T, oldFile, newData interface{}) []string {
	// should be skipped in json comparison after passing binary comparison
	var toSkip []string

	if err := traverseAndCompare(oldFile, newData, "/", &toSkip); err != nil {
		t.Fatalf("Reference files differ: %v", err)
	}
	return toSkip
}

func isExternalFile(value interface{}) bool {
	if strings.Contains(fmt.Sprintf("%v", value), "uri") && strings.Contains(fmt.Sprintf("%v", value), "https://") {
		return true
	}
	return false
}

func containsLocalFiles(value interface{}) bool {
	if strings.Contains(fmt.Sprintf("%v", value), "uri") && strings.Contains(fmt.Sprintf("%v", value), "file://") {
		return true
	}
	return false
}

func compareBinary(fileContent []byte, currentContent interface{}) error {
	if diff := cmp.Diff(strings.TrimSpace(string(fmt.Sprintf("%v", currentContent))), strings.TrimSpace(string(fileContent))); diff != "" {
		return fmt.Errorf("file content does not match expected content (-expected +file):\n%s", diff)
	}
	return nil
}

func readFileContent(uri string) ([]byte, error) {
	fullFilePath, err := localFilePath(uri)
	if err != nil {
		return nil, xerrors.Errorf("failed to assemble local reference filePath: %w", err)
	}

	return os.ReadFile(fullFilePath)
}

func localFilePath(uri string) (string, error) {
	filePath := strings.TrimPrefix(uri, "file://")

	currentFilePath, err := getCallingTestFile()
	if err != nil {
		return "", xerrors.Errorf("failed to find reference file filepath: %w", err)
	}

	testDir := filepath.Dir(currentFilePath)
	dir := strings.Split(filePath, ".")[0]
	testDir = strings.ReplaceAll(testDir, "/-S/", "")
	rootPath, _ := FindRoot(currentFilePath)
	if rootPath != "" {
		testDir = strings.ReplaceAll(testDir, rootPath+"/", "")
	}
	testDir = yatest.SourcePath(testDir)

	var fullFilePath string
	if dir == gotestDir {
		fullFilePath = filepath.Join(testDir, gotestDir, canonDir, filePath)
	} else {
		fullFilePath = filepath.Join(testDir, canonDir, filePath)
	}
	return fullFilePath, nil
}

func getCallingTestFile() (string, error) {
	for i := 0; i < 30; i++ {
		_, currentFilePath, _, ok := runtime.Caller(i)
		if !ok {
			return "", xerrors.New("unable to get current file path")
		}
		if strings.HasSuffix(currentFilePath, "_test.go") {
			return currentFilePath, nil
		}
	}

	return "", xerrors.New("could not find caller _test.go filePath")
}

func traverseAndCompare(referenceData interface{}, currentData interface{}, basePath string, toSkip *[]string) error {
	switch v := referenceData.(type) {
	case map[string]interface{}:
		for key, value := range v {
			if key == "uri" {
				uriStr := value.(string)
				referenceFileData, err := readFileContent(uriStr)
				if err != nil {
					return xerrors.Errorf("error reading file content from URI %s: %v\n", uriStr, err)
				}
				jsonPath := basePath[:len(basePath)-1]
				*toSkip = append(*toSkip, jsonPath)
				if err := compareBinary(referenceFileData, currentData); err != nil {
					// if update then write to file otherwise error
					if *update {
						if err := updateLocalReferenceFile(uriStr, currentData); err != nil {
							return xerrors.Errorf("failed to regenerate local reference file: %s", uriStr)
						}
					} else {
						return xerrors.Errorf("mismatch found for path: %s in file: %s, err: %v\n", jsonPath, uriStr, err)
					}
				}
			} else {
				innerCurrentData, err := getData(currentData, key, 0)
				if err != nil {
					if *update {
						continue
					}
					return xerrors.Errorf("current data and reference data differ for path %s : %w", basePath, err)
				}
				if err := traverseAndCompare(value, innerCurrentData, basePath+key+"/", toSkip); err != nil {
					return xerrors.Errorf("failed to recursively check for local file: %w", err)
				}
			}
		}
	case []interface{}:
		for i, item := range v {
			innerCurrentData, err := getData(currentData, "", i)
			if err != nil {
				if *update {
					continue
				}
				return xerrors.Errorf("current data and reference data differ for path %s : %w", basePath, err)
			}

			if err := traverseAndCompare(item, innerCurrentData, fmt.Sprintf("%s%d/", basePath, i), toSkip); err != nil {
				return xerrors.Errorf("failed to recursively check for local file: %w", err)
			}
		}
	}
	return nil
}

func getData(data interface{}, key string, index int) (interface{}, error) {
	switch v := data.(type) {
	case map[string]interface{}:
		value, ok := v[key]
		if !ok {
			return nil, xerrors.Errorf("missing value for key: %s", key)
		}
		return value, nil

	case []interface{}:
		if index >= len(v) {
			return nil, xerrors.Errorf("found different lengths in JSON array, current array length: %d, reference array length: %d", len(v), index)
		}
		return v[index], nil
	}
	return data, nil
}

func prettyPrintDiff(patch jsondiff.Patch, testName string) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Test: %s\n", testName))

	// avoid showing more than 5 changes per test just for sanity
	previewLimit := 5
	for _, op := range patch {
		previewLimit--
		op.Path = strings.TrimPrefix(op.Path, "/")
		switch op.Type {
		case jsondiff.OperationAdd:
			b.WriteString(fmt.Sprintf("+ %s: %v\n", op.Path, op.Value))
		case jsondiff.OperationRemove:
			b.WriteString(fmt.Sprintf("- %s: %v\n", op.Path, op.OldValue))
		case jsondiff.OperationReplace:
			b.WriteString(fmt.Sprintf("~ %s: %v -> %v\n", op.Path, op.OldValue, op.Value))
		case jsondiff.OperationMove:
			b.WriteString(fmt.Sprintf("> %s -> %s\n", op.From, op.Path))
		case jsondiff.OperationCopy:
			b.WriteString(fmt.Sprintf("< %s -> %s\n", op.From, op.Path))
		case jsondiff.OperationTest:
			b.WriteString(fmt.Sprintf("? %s: %v\n", op.Path, op.Value))
		}
		if previewLimit == 0 {
			// enough is enough
			break
		}
	}

	return b.String()
}

func toUpdateReferenceObject(referenceData, currentData interface{}, skipped []string) (interface{}, error) {
	currentDataMap, ok := currentData.(map[string]interface{})
	if ok {
		// current data is a map
		for _, val := range skipped {
			path := strings.TrimPrefix(val, "/")
			nestings := strings.Split(path, "/")
			key := nestings[0]

			tmpNestedRef, err := getData(referenceData, key, 0)
			if err != nil {
				return nil, xerrors.Errorf("failed to get data from reference")
			}
			currentDataMap[key] = tmpNestedRef //
		}
		return currentDataMap, nil
	}

	currentDataArray, ok := currentData.([]interface{})
	if ok {
		// current data is array
		for index := range currentDataArray {
			for _, val := range skipped {
				path := strings.TrimPrefix(val, "/")
				nestings := strings.Split(path, "/")
				strIndex := nestings[0]
				intIndex, err := strconv.Atoi(strIndex)
				if err == nil && index == intIndex {
					tmpNestedRef, err := getData(referenceData, "", intIndex)
					if err != nil {
						return nil, xerrors.Errorf("failed to get data from reference")
					}

					currentDataArray[intIndex] = tmpNestedRef
				}
			}
		}
		return currentDataArray, nil
	}

	if len(skipped) == 1 && skipped[0] == "" {
		// full result was skipped, probably ony file uri in result.json
		return referenceData, nil
	}

	return currentData, nil
}

func updateLocalReferenceFile(uri string, data interface{}) error {
	filePath, err := localFilePath(uri)
	if err != nil {
		return xerrors.Errorf("failed to assemble local reference filePath: %w", err)
	}
	err = os.WriteFile(filePath, []byte(fmt.Sprintf("%v", data)), 0o644)
	if err != nil {
		return xerrors.Errorf("Failed to write regenerated JSON file: %w", err)
	}
	return nil
}

func updateReference(t *testing.T, referenceFilePath string, referenceData map[string]interface{}) {
	if *update {
		sortedKeys := make([]string, 0, len(referenceData))
		for key := range referenceData {
			sortedKeys = append(sortedKeys, key)
		}
		sort.Strings(sortedKeys)

		sortedData := make(map[string]interface{})
		for _, key := range sortedKeys {
			sortedData[key] = referenceData[key]
		}

		sortedJSONData, err := json.MarshalIndent(sortedData, "", "    ")
		if err != nil {
			t.Fatalf("Failed to marshal sorted JSON data: %v", err)
		}

		err = os.WriteFile(referenceFilePath, append(sortedJSONData, '\n'), 0o644)
		if err != nil {
			t.Fatalf("Failed to write regenerated JSON file: %v", err)
		}

		t.Logf("Regenerated canon file: %s", referenceFilePath)
	}
}

func getCanonDestination(t *testing.T, canonFilename string) string {
	var err error
	if _, err = os.Stat(canonFilename); os.IsNotExist(err) {
		t.Fatalf("file: %s does not exists", canonFilename)
	}

	tmpDir := t.TempDir()

	dstFilename := filepath.Join(tmpDir, filepath.Base(canonFilename))
	return dstFilename
}

func SaveFile(t *testing.T, canonFilename string, opts ...Option) {
	if yatest.HasYaTestContext() {
		canon.SaveFile(t, canonFilename)
		return
	}
	dstFilename := getCanonDestination(t, canonFilename)
	dst, err := os.Create(dstFilename)
	if err != nil {
		t.Fatalf("failed to create destination file %s: %v", dstFilename, err)
	}
	defer dst.Close()

	src, err := os.Open(canonFilename)
	if err != nil {
		t.Fatalf("failed to open %s: %v", canonFilename, err)
	}
	defer src.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		t.Fatalf("failed to copy file: %v", err)
	}

	uri := "file://" + dstFilename
	canonDescription := fileObject{
		Local:    nil,
		URI:      uri,
		DiffTool: []string{},
	}
	for _, opt := range opts {
		switch v := opt.(type) {
		case diffToolOption:
			if canonDescription.DiffTool == nil {
				canonDescription.DiffTool = v.diffTool
			} else {
				t.Fatalf("option %#v already set", opt)
			}
		case isLocalOption:
			if canonDescription.Local == nil {
				canonDescription.Local = new(bool)
				*canonDescription.Local = v.isLocal
			} else {
				t.Fatalf("option %#v already set", opt)
			}
		default:
			t.Fatalf("unexpected option %#v", opt)
		}
	}
	SaveJSON(t, canonDescription)
}
