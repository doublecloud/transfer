package main

import (
	_ "embed"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"text/template"

	"github.com/doublecloud/transfer/cloud/dataplatform/api/options"
	"github.com/doublecloud/transfer/cloud/dataplatform/protogen/pkg/gogen"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/library/go/slices"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

var (
	//go:embed validating_service_wrapper.tmpl
	serviceWrapperTemplateData string

	serviceWrapperTemplate = template.Must(template.New("validating_service_wrapper.tmpl").Parse(serviceWrapperTemplateData))

	varCounter = uint64(0)
)

func getNextVarCounter() uint64 {
	varCounter++
	return varCounter
}

func getFieldInfo(field *descriptorpb.FieldDescriptorProto) *options.FieldInfo {
	fieldInfo, ok := proto.GetExtension(field.GetOptions(), options.E_FieldInfo).(*options.FieldInfo)
	if !ok {
		panic("No FieldInfo extension registered")
	}
	return fieldInfo
}

func getMethodInfo(field *descriptorpb.MethodDescriptorProto) *options.MethodInfo {
	methodInfo, ok := proto.GetExtension(field.GetOptions(), options.E_MethodInfo).(*options.MethodInfo)
	if !ok {
		panic("No MethodInfo extension registered")
	}
	return methodInfo
}

func getOneofInfo(field *descriptorpb.OneofDescriptorProto) *options.OneofInfo {
	oneofInfo, ok := proto.GetExtension(field.GetOptions(), options.E_OneofInfo).(*options.OneofInfo)
	if !ok {
		panic("No OneofInfo extension registered")
	}
	return oneofInfo
}

type messageKey struct {
	packageName string
	messageName string
}

func (k messageKey) fqmn() fqmn {
	return fqmn(fmt.Sprintf(".%s.%s", k.packageName, k.messageName))
}

type fqmn string

type messageEntry struct {
	msg  *descriptorpb.DescriptorProto
	file *descriptorpb.FileDescriptorProto
}

func hasValidatableFields(
	msgEntry messageEntry,
	validatableMessages map[fqmn]messageEntry,
) bool {
	for _, field := range msgEntry.msg.GetField() {
		customValidations := getCustomValidations(field)
		for _, validation := range customValidations {
			if !validation.GetNoValidate() {
				return true
			}
		}
		if validation := getFieldInfo(field).GetValidation(); validation != nil {
			if validation.GetNoValidate() {
				continue
			}
			return true
		}
		if field.GetType() == descriptorpb.FieldDescriptorProto_TYPE_ENUM {
			return true
		}
		if field.GetType() == descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
			if _, ok := validatableMessages[fqmn(field.GetTypeName())]; ok {
				return true
			}
		}
	}
	for _, oneof := range msgEntry.msg.GetOneofDecl() {
		if getOneofInfo(oneof).GetValidation() != nil {
			return true
		}
	}
	return false
}

func listMessages(allFiles []*descriptorpb.FileDescriptorProto, bodyBuilder *strings.Builder) (
	validatableMessages map[fqmn]messageEntry,
	allMessages map[fqmn]messageEntry,
	goPackages map[protoPackageName]goPackage,
	err error,
) {
	validatableMessages = map[fqmn]messageEntry{}
	allMessages = map[fqmn]messageEntry{}
	goPackages = map[protoPackageName]goPackage{}
	messagesToBeChecked := map[fqmn]messageEntry{}
	for _, file := range allFiles {
		goPkg, err := parseGoPackage(file.GetOptions().GetGoPackage())
		if err != nil {
			return nil, nil, nil, xerrors.Errorf("file %s: cannot parse go package option %s: %w", file.GetName(), file.GetOptions().GetGoPackage(), err)
		}
		goPackages["."+file.GetPackage()] = *goPkg
		for _, msg := range file.MessageType {
			for _, nestedMsg := range msg.GetNestedType() {
				key := messageKey{packageName: file.GetPackage(), messageName: fmt.Sprintf("%s.%s", msg.GetName(), nestedMsg.GetName())}
				entry := messageEntry{msg: nestedMsg, file: file}
				allMessages[key.fqmn()] = entry
				messagesToBeChecked[key.fqmn()] = entry
			}
			key := messageKey{packageName: file.GetPackage(), messageName: msg.GetName()}
			entry := messageEntry{msg: msg, file: file}
			allMessages[key.fqmn()] = entry
			messagesToBeChecked[key.fqmn()] = entry
		}
	}

	for {
		newValidatable := 0
		for fqmn, messageEntry := range messagesToBeChecked {
			if hasValidatableFields(messageEntry, validatableMessages) {
				newValidatable++
				validatableMessages[fqmn] = messageEntry
				delete(messagesToBeChecked, fqmn)
			}
		}
		if newValidatable == 0 {
			break
		}
	}

	return validatableMessages, allMessages, goPackages, nil
}

func writeMessageFieldCheck(field *descriptorpb.FieldDescriptorProto, builder *strings.Builder) {
	camelName := gogen.GoCamelCase(field.GetName())
	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		builder.WriteString(fmt.Sprintf("\tfor i, submsg := range msg.Get%s() {\n", camelName))
		builder.WriteString("\t\tif err := submsg.Validate(); err != nil {\n")
		builder.WriteString(fmt.Sprintf("\t\t\treturn xerrors.Errorf(\"%s[%s]: %s\", i, err)\n", field.GetName(), "%v", "%w"))
		builder.WriteString("\t\t}\n")
		builder.WriteString("\t}\n")
	} else {
		builder.WriteString(fmt.Sprintf("\tif msg.Get%s() != nil {\n", camelName))
		builder.WriteString(fmt.Sprintf("\t\tif err := msg.Get%s().Validate(); err != nil {\n", camelName))
		builder.WriteString(fmt.Sprintf("\t\t\treturn xerrors.Errorf(\"%s: %s\", err)\n", field.GetName(), "%w"))
		builder.WriteString("\t\t}\n")
		builder.WriteString("\t}\n")
	}
}

type protoPackageName = string

type goPackage struct {
	importPath  importPath
	importAlias importAlias
}

func (p *goPackage) qualifyIdentifier(identifier string) string {
	qualificator := string(p.importAlias)
	if qualificator == "" {
		qualificator = filepath.Base(string(p.importPath))
	}
	return qualificator + "." + identifier
}

func parseGoPackage(goPackageProtoOption string) (*goPackage, error) {
	components := strings.Split(goPackageProtoOption, ";") // len(components) is always > 0
	if len(components) > 2 {
		return nil, xerrors.Errorf("invalid format: more than one semicolon found")
	}
	path := importPath(components[0])
	var alias importAlias
	if len(components) == 2 {
		alias = importAlias(components[1])
	}
	return &goPackage{importPath: path, importAlias: alias}, nil
}

type importPath string
type importAlias string
type importMap map[importPath]importAlias

func withLeadingDot(packageName string) string {
	if len(packageName) == 0 {
		return "."
	}
	if packageName[0] == '.' {
		return packageName
	}
	return "." + packageName
}

func goTypeName(
	fqtn string,
	sourceFilePackage string,
	allMessages map[fqmn]messageEntry,
	imports importMap,
	goPackages map[protoPackageName]goPackage,
) string {
	typePathComponents := strings.Split(fqtn, ".")
	var goTypeName string
	if len(typePathComponents) == 0 {
		return fqtn
	}
	goTypeName = typePathComponents[len(typePathComponents)-1]

	i := len(typePathComponents) - 1
	for ; i >= 0; i-- {
		if _, ok := allMessages[fqmn(strings.Join(typePathComponents[:i], "."))]; !ok {
			// No nesting message found
			// goTypeName += "_name"
			break
		}
		goTypeName = typePathComponents[i-1] + "_" + goTypeName
	}
	protoPackage := strings.Join(typePathComponents[:i], ".")
	if withLeadingDot(protoPackage) == withLeadingDot(sourceFilePackage) {
		return goTypeName
	}
	goPackage, ok := goPackages[protoPackage]
	if !ok {
		panic(fmt.Sprintf("No Go package found for proto package %s", protoPackage))
	}
	imports[goPackage.importPath] = goPackage.importAlias
	return goPackage.qualifyIdentifier(goTypeName)
}

func enumNamesMap(
	fqtn string,
	sourceFilePackage string,
	allMessages map[fqmn]messageEntry,
	imports importMap,
	goPackages map[protoPackageName]goPackage,
) string {
	return goTypeName(fqtn, sourceFilePackage, allMessages, imports, goPackages) + "_name"
}

func writeEnumFieldCheck(
	field *descriptorpb.FieldDescriptorProto,
	sourceFilePackage string,
	allMessages map[fqmn]messageEntry,
	builder *strings.Builder,
	imports importMap,
	goPackages map[protoPackageName]goPackage,
) {
	camelName := gogen.GoCamelCase(field.GetName())
	enumNamesMap := enumNamesMap(field.GetTypeName(), sourceFilePackage, allMessages, imports, goPackages)
	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		builder.WriteString(fmt.Sprintf("\tfor i, enumValue := range msg.Get%s() {\n", camelName))
		builder.WriteString(fmt.Sprintf("\t\tif _, ok := %s[int32(enumValue)]; !ok {\n", enumNamesMap))
		builder.WriteString(fmt.Sprintf("\t\t\treturn xerrors.Errorf(\"%s[%s]: unknown enum value %s\", i, enumValue)\n", field.GetName(), "%d", "%d"))
		builder.WriteString("\t\t}\n")
		builder.WriteString("\t}\n")
	} else {
		builder.WriteString(fmt.Sprintf("\tif _, ok := %s[int32(msg.Get%s())]; !ok {\n", enumNamesMap, camelName))
		builder.WriteString(fmt.Sprintf("\t\treturn xerrors.Errorf(\"%s: unknown enum value %s\", msg.Get%s())\n", field.GetName(), "%d", camelName))
		builder.WriteString("\t}\n")
	}
}

func getStringFieldLengthCode(varName string, minLen, maxLen int64, tabs int, errorExpr string) (string, error) {
	var condition string
	if minLen == 0 && maxLen == 0 {
		return "", xerrors.Errorf("field \"%s\" has limits: minimum=0 & maximum=0", varName)
	} else if minLen != 0 && maxLen == 0 {
		// We use []rune to sucessfully proceed non-ASCII letters.
		// I.e., cloud's folder_id has no restricts to input language.
		condition = fmt.Sprintf("if len([]rune(%s))<%d {", varName, minLen)
		errorExpr = fmt.Sprintf(errorExpr, fmt.Sprintf("[%d..)", minLen))
	} else if minLen == 0 && maxLen != 0 {
		condition = fmt.Sprintf("if len([]rune(%s))>%d {", varName, maxLen)
		errorExpr = fmt.Sprintf(errorExpr, fmt.Sprintf("[0..%d]", maxLen))
	} else {
		condition = fmt.Sprintf("if len([]rune(%s))>%d || len([]rune(%s))<%d {", varName, maxLen, varName, minLen)
		errorExpr = fmt.Sprintf(errorExpr, fmt.Sprintf("[%d..%d]", minLen, maxLen))
	}
	result := []string{condition, "\treturn " + errorExpr, "}\n"}
	for i := range result {
		result[i] = strings.Repeat("\t", tabs) + result[i]
	}
	return strings.Join(result, "\n"), nil
}

func writeStringFieldLengthCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	minLen int64,
	maxLen int64,
) error {
	switch field.GetType() {
	default:
		return xerrors.Errorf("field %s is not type string, but used with string_length_range. field type: %s", field.GetName(), descriptorpb.FieldDescriptorProto_Type_name[int32(field.GetType())])
	case descriptorpb.FieldDescriptorProto_TYPE_STRING:
	}

	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		builder.WriteString(indent + fmt.Sprintf("for i, strValue := range msg.Get%s() {\n", camelName))
		str, err := getStringFieldLengthCode("strValue", minLen, maxLen, tabs+1, fmt.Sprintf("xerrors.Errorf(\"Length of \\\"%s[%s]\\\" is outside allowed range %%s\", i)", field.GetName(), "%%d"))
		if err != nil {
			return err
		}
		builder.WriteString(str)
		builder.WriteString(indent + "}\n")
	} else {
		str, err := getStringFieldLengthCode(fmt.Sprintf("msg.Get%s()", camelName), minLen, maxLen, tabs, fmt.Sprintf("xerrors.New(\"Length of \\\"%s\\\" is outside allowed range %%s\")", field.GetName()))
		if err != nil {
			return err
		}
		builder.WriteString(str)
	}
	return nil
}

func writeDoubleFieldRangeCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	minVal float64,
	maxVal float64,
) error {
	if field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_DOUBLE {
		return xerrors.Errorf("field type is %s but double_range validation is used", descriptorpb.FieldDescriptorProto_Type_name[int32(field.GetType())])
	}

	params := FloatCheckParams{Min: minVal, Max: maxVal}
	var writer CheckWriter[FloatCheckParams] = &FloatCheckWriter{}

	return writeGenericCheck(field, builder, tabs, writer, &params)
}

var intTypes = []descriptorpb.FieldDescriptorProto_Type{
	descriptorpb.FieldDescriptorProto_TYPE_INT32,
	descriptorpb.FieldDescriptorProto_TYPE_INT64,
}

var intTypeNames = []string{
	".google.protobuf.Int64Value",
}

func writeIntFieldRangeCheck(
	field *descriptorpb.FieldDescriptorProto, builder *strings.Builder, tabs int, minVal, maxVal int64,
) error {
	params := IntCheckParams{Min: minVal, Max: maxVal}
	var writer CheckWriter[IntCheckParams] = nil

	if slices.Contains(intTypes, field.GetType()) {
		writer = &IntCheckWriter{}
	}
	if slices.Contains(intTypeNames, field.GetTypeName()) {
		writer = &OptionalIntCheckWriter{}
	}

	if writer == nil {
		return xerrors.Errorf("field type is %s (TypeName: %s) but int_range validation is used",
			descriptorpb.FieldDescriptorProto_Type_name[int32(field.GetType())], field.GetTypeName())
	}
	return writeGenericCheck(field, builder, tabs, writer, &params)
}

func writeGenericCheck[T any](
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	writer CheckWriter[T],
	params *T,
) error {
	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		varName := "itemValue"
		builder.WriteString(indent + fmt.Sprintf("for i, %s := range msg.Get%s() {\n", varName, camelName))
		builder.WriteString(writer.Write(varName, params, tabs+1, fmt.Sprintf("xerrors.Errorf(\"Value of \\\"%s[%%%%v]\\\" %%s\", i)", field.GetName())))
		builder.WriteString(indent + "}\n")
	} else {
		builder.WriteString(writer.Write(fmt.Sprintf("msg.Get%s()", camelName), params, tabs, fmt.Sprintf("xerrors.Errorf(\"Value of \\\"%s\\\" %%s\")", field.GetName())))
	}
	return nil
}

func writeRepeatableFieldRangeCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	minLen int64,
	maxLen int64,
) error {
	if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		return xerrors.Errorf("field %s is not repeatable, but used with repeated_field_length.", field.GetName())
	}

	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	if minLen == 0 && maxLen == 0 {
		return xerrors.Errorf("field \"%s\" has minimum and maximum equal zero", field.GetName())
	} else if (minLen != 0) && (maxLen == 0) {
		builder.WriteString(fmt.Sprintf(indent+"if len(msg.Get%s())<%d {\n", camelName, minLen))
	} else if (minLen == 0) && (maxLen != 0) {
		builder.WriteString(fmt.Sprintf(indent+"if len(msg.Get%s())>%d {\n", camelName, maxLen))
	} else if (minLen != 0) && (maxLen != 0) {
		builder.WriteString(fmt.Sprintf(indent+"if len(msg.Get%s())<%d || len(msg.Get%s())>%d {\n", camelName, minLen, camelName, maxLen))
	}
	builder.WriteString(fmt.Sprintf(indent+"\treturn xerrors.New(\"Count of repeated field \\\"%s\\\" is outside allowed range [%d, %d]\")\n", field.GetName(), minLen, maxLen))
	builder.WriteString(indent + "}\n")
	return nil
}

func writeMapLengthCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	minLen int64,
	maxLen int64,
) error {
	if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		return xerrors.Errorf("field %s is not repeatable, but used with map_length.", field.GetName())
	}
	if field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		return xerrors.Errorf("field %s is not message, but used with map_length.", field.GetName())
	}

	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	if minLen == 0 && maxLen == 0 {
		return xerrors.Errorf("field \"%s\" has minimum and maximum equal zero", field.GetName())
	} else if (minLen != 0) && (maxLen == 0) {
		builder.WriteString(fmt.Sprintf(indent+"if len(msg.Get%s())<%d {\n", camelName, minLen))
		builder.WriteString(fmt.Sprintf(indent+"\treturn xerrors.New(\"Length of map \\\"%s\\\" is outside allowed range [%d..)\")\n", field.GetName(), minLen))
	} else if (minLen == 0) && (maxLen != 0) {
		builder.WriteString(fmt.Sprintf(indent+"if len(msg.Get%s())>%d {\n", camelName, maxLen))
		builder.WriteString(fmt.Sprintf(indent+"\treturn xerrors.New(\"Length of map \\\"%s\\\" is outside allowed range [0..%d]\")\n", field.GetName(), maxLen))
	} else if (minLen != 0) && (maxLen != 0) {
		builder.WriteString(fmt.Sprintf(indent+"if len(msg.Get%s())<%d || len(msg.Get%s())>%d {\n", camelName, minLen, camelName, maxLen))
		builder.WriteString(fmt.Sprintf(indent+"\treturn xerrors.New(\"Length of map \\\"%s\\\" is outside allowed range [%d..%d]\")\n", field.GetName(), minLen, maxLen))
	}
	builder.WriteString(indent + "}\n")
	return nil
}

func getRegexCheckCode(regex string, val string, tabs int, valPrefix string) string {
	result := []string{
		fmt.Sprintf("if matched, _ := regexp.MatchString(`%s`, %s); !matched {", regex, val),
		fmt.Sprintf("\treturn xerrors.New(`%s does not match regular expression %s`)", valPrefix, regex),
		"}\n",
	}
	for i := range result {
		result[i] = strings.Repeat("\t", tabs) + result[i]
	}
	return strings.Join(result, "\n")
}

func getRegexCheckCodePrecompiled(varName string, val string, tabs int, valPrefix string) string {
	result := []string{
		fmt.Sprintf("if matched := %s.MatchString(%s); !matched {", varName, val),
		fmt.Sprintf("\treturn xerrors.Errorf(`%s does not match regular expression %%s`, %s.String())", valPrefix, varName),
		"}\n",
	}
	for i := range result {
		result[i] = strings.Repeat("\t", tabs) + result[i]
	}
	return strings.Join(result, "\n")
}

func writeFieldRegexCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	regex string,
) error {
	switch field.GetType() {
	default:
		return xerrors.Errorf("field %s is not type string, but used with regex. field type: %s", field.GetName(), descriptorpb.FieldDescriptorProto_Type_name[int32(field.GetType())])
	case descriptorpb.FieldDescriptorProto_TYPE_STRING:
	}

	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	_, err := regexp.MatchString("", regex)
	if err != nil {
		return xerrors.Errorf("invalid regex for field: %s, regex: %s", field.GetName(), regex)
	}

	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		regexVarName := fmt.Sprintf("regexValCheck%d", getNextVarCounter())
		builder.WriteString(fmt.Sprintf(indent+"%s, _ := regexp.Compile(`%s`)\n", regexVarName, regex))
		builder.WriteString(fmt.Sprintf(indent+"for _, strValue := range msg.Get%s() {\n", camelName))
		builder.WriteString(getRegexCheckCodePrecompiled(regexVarName, "strValue", tabs+1, fmt.Sprintf(`Field "%s"`, field.GetName())))
		builder.WriteString(indent + "}\n")
	} else {
		builder.WriteString(getRegexCheckCode(regex, fmt.Sprintf("msg.Get%s()", camelName), tabs, fmt.Sprintf(`Field "%s"`, field.GetName())))
	}

	return nil
}

func writeKeyLengthCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	minLen int64,
	maxLen int64,
) error {
	if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		return xerrors.Errorf("field %s is not repeatable, but used with key_length.", field.GetName())
	}
	if field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		return xerrors.Errorf("field %s is not message, but used with key_length.", field.GetName())
	}

	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	builder.WriteString(fmt.Sprintf(indent+"for key := range msg.Get%s() {\n", camelName))
	if minLen == 0 && maxLen == 0 {
		return xerrors.Errorf("field \"%s\" has minimum and maximum equal zero", field.GetName())
	} else if (minLen != 0) && (maxLen == 0) {
		builder.WriteString(fmt.Sprintf(indent+"\tif len(key)<%d {\n", minLen))
		builder.WriteString(fmt.Sprintf(indent+"\t\treturn xerrors.Errorf(\"Length of key in map \\\"%s\\\" is outside allowed range [%d..), key: %%s\", key)\n", field.GetName(), minLen))
	} else if (minLen == 0) && (maxLen != 0) {
		builder.WriteString(fmt.Sprintf(indent+"\tif len(key)>%d {\n", maxLen))
		builder.WriteString(fmt.Sprintf(indent+"\t\treturn xerrors.Errorf(\"Length of key in map \\\"%s\\\" is outside allowed range [0..%d], key: %%s\", key)\n", field.GetName(), maxLen))
	} else if (minLen != 0) && (maxLen != 0) {
		builder.WriteString(fmt.Sprintf(indent+"\tif len(key)<%d || len(key)>%d {\n", minLen, maxLen))
		builder.WriteString(fmt.Sprintf(indent+"\t\treturn xerrors.Errorf(\"Length of key in map \\\"%s\\\" is outside allowed range [%d..%d], key: %%s\", key)\n", field.GetName(), minLen, maxLen))
	}
	builder.WriteString(indent + "\t}\n")
	builder.WriteString(indent + "}\n")

	return nil
}

func writeValueLengthCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	minLen int64,
	maxLen int64,
) error {
	if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		return xerrors.Errorf("field %s is not repeatable, but used with value_length.", field.GetName())
	}
	if field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		return xerrors.Errorf("field %s is not message, but used with value_length.", field.GetName())
	}

	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	builder.WriteString(fmt.Sprintf("\tfor _, value := range msg.Get%s() {\n", camelName))
	if minLen == 0 && maxLen == 0 {
		return xerrors.Errorf("field \"%s\" has minimum and maximum equal zero", field.GetName())
	} else if (minLen != 0) && (maxLen == 0) {
		builder.WriteString(fmt.Sprintf(indent+"\tif len(msg.Get%s())<%d {\n", camelName, minLen))
		builder.WriteString(fmt.Sprintf(indent+"\t\treturn xerrors.Errorf(\"Length of value in map \\\"%s\\\" is outside allowed range [%d..), value: %%s\", value)\n", field.GetName(), minLen))
	} else if (minLen == 0) && (maxLen != 0) {
		builder.WriteString(fmt.Sprintf(indent+"\tif len(msg.Get%s())>%d {\n", camelName, maxLen))
		builder.WriteString(fmt.Sprintf(indent+"\t\treturn xerrors.Errorf(\"Length of value in map \\\"%s\\\" is outside allowed range [0..%d], value: %%s\", value)\n", field.GetName(), maxLen))
	} else if (minLen != 0) && (maxLen != 0) {
		builder.WriteString(fmt.Sprintf(indent+"\tif len(msg.Get%s())<%d || len(msg.Get%s())>%d {\n", camelName, minLen, camelName, maxLen))
		builder.WriteString(fmt.Sprintf(indent+"\t\treturn xerrors.Errorf(\"Length of value in map \\\"%s\\\" is outside allowed range [%d..%d], value: %%s\", value)\n", field.GetName(), minLen, maxLen))
	}
	builder.WriteString(indent + "\t}\n")
	builder.WriteString(indent + "}\n")

	return nil
}

func writeKeyRegexCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	regex string,
) error {
	if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		return xerrors.Errorf("field %s is not repeatable, but used with key_regex.", field.GetName())
	}
	if field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		return xerrors.Errorf("field %s is not message, but used with key_regex.", field.GetName())
	}

	_, err := regexp.MatchString("", regex)
	if err != nil {
		return xerrors.Errorf("invalid regex for field: %s, regex: %s", field.GetName(), regex)
	}

	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		regexVarName := fmt.Sprintf("regexKeyCheck%d", getNextVarCounter())
		builder.WriteString(fmt.Sprintf(indent+"%s, _ := regexp.Compile(`%s`)\n", regexVarName, regex))
		builder.WriteString(fmt.Sprintf(indent+"for key := range msg.Get%s() {\n", camelName))
		builder.WriteString(getRegexCheckCodePrecompiled(regexVarName, "key", 2, fmt.Sprintf(`Key in map "%s"`, field.GetName())))
		builder.WriteString(indent + "}\n")
	} else {
		builder.WriteString(getRegexCheckCode(regex, "msg."+gogen.GoCamelCase(field.GetName()), tabs, fmt.Sprintf(`Key in map "%s"`, field.GetName())))
	}

	return nil
}

func writeValueRegexCheck(
	field *descriptorpb.FieldDescriptorProto,
	builder *strings.Builder,
	tabs int,
	regex string,
) error {
	if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		return xerrors.Errorf("field %s is not repeatable, but used with key_regex.", field.GetName())
	}
	if field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		return xerrors.Errorf("field %s is not message, but used with key_regex.", field.GetName())
	}

	_, err := regexp.MatchString("", regex)
	if err != nil {
		return xerrors.Errorf("invalid regex for field: %s, regex: %s", field.GetName(), regex)
	}

	camelName := gogen.GoCamelCase(field.GetName())
	indent := strings.Repeat("\t", tabs)

	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		regexVarName := fmt.Sprintf("regexValCheck%d", getNextVarCounter())
		builder.WriteString(fmt.Sprintf(indent+"%s, _ := regexp.Compile(`%s`)\n", regexVarName, regex))
		builder.WriteString(fmt.Sprintf(indent+"for _, value := range msg.Get%s() {\n", camelName))
		builder.WriteString(getRegexCheckCodePrecompiled(regexVarName, "value", tabs, fmt.Sprintf(`Value in map "%s"`, field.GetName())))
		builder.WriteString(indent + "}\n")
	} else {
		builder.WriteString(getRegexCheckCode(regex, "msg."+gogen.GoCamelCase(field.GetName()), tabs, fmt.Sprintf(`Value in map "%s"`, field.GetName())))
	}

	return nil
}

func writeRequiredCheck(field *descriptorpb.FieldDescriptorProto, builder *strings.Builder, tabs int) {
	indent := strings.Repeat("\t", tabs)
	writeFieldCodeCheck("FieldNotExists", "msg", field, builder, tabs, false)
	builder.WriteString(fmt.Sprintf(indent+"if %s {\n", gogen.GoCamelCase(field.GetName())+"FieldNotExists"))
	builder.WriteString(fmt.Sprintf(indent+"\treturn xerrors.New(\"Missing required field \\\"%s\\\"\")\n", field.GetName()))
	builder.WriteString(indent + "}\n")
}

func writeComputedCheck(field *descriptorpb.FieldDescriptorProto, builder *strings.Builder, tabs int) {
	indent := strings.Repeat("\t", tabs)
	writeFieldCodeCheck("FieldExists", "msg", field, builder, tabs, true)
	builder.WriteString(fmt.Sprintf(indent+"if %s {\n", gogen.GoCamelCase(field.GetName())+"FieldExists"))
	builder.WriteString(fmt.Sprintf(indent+"\treturn xerrors.New(\"Computed field specified \\\"%s\\\"\")\n", field.GetName()))
	builder.WriteString(indent + "}\n")
}

func writeLogicalOneofChecks(fields []*descriptorpb.FieldDescriptorProto, builder *strings.Builder, tabs int) error {
	oneofsFields := make(map[string][]*descriptorpb.FieldDescriptorProto)
	oneofsTypes := make(map[string]options.Oneof_Mode)
	for _, field := range fields {
		validationInfo := getFieldInfo(field).GetValidation()
		if validationInfo == nil {
			continue
		}
		if oneof := validationInfo.Oneof; oneof != nil {
			oneofsFields[oneof.Group] = append(oneofsFields[oneof.Group], field)
			if len(oneofsFields[oneof.Group]) == 1 {
				oneofsTypes[oneof.Group] = oneof.Mode
			} else if oneofsTypes[oneof.Group] != oneof.Mode {
				mode1 := options.Oneof_Mode_name[int32(oneofsTypes[oneof.Group])]
				mode2 := options.Oneof_Mode_name[int32(oneof.Mode)]
				return xerrors.Errorf("oneof %s has 2 different modes: %s, %s", oneof.Group, mode1, mode2)
			}
		}
	}
	for group, groupFields := range oneofsFields {
		writeLogicalOneofCheck(groupFields, oneofsTypes[group], group, builder, tabs)
	}
	return nil
}

func writeLogicalOneofCheck(fields []*descriptorpb.FieldDescriptorProto, mode options.Oneof_Mode, group string, builder *strings.Builder, tabs int) {
	indent := strings.Repeat("\t", tabs+1)
	builder.WriteString(fmt.Sprintf("%s// %s oneof check block\n", strings.Repeat("\t", tabs), group))
	builder.WriteString(fmt.Sprintf("%s{\n", strings.Repeat("\t", tabs)))
	builder.WriteString(fmt.Sprintf(indent+"existing%sOneofFields := make([]string, 0)\n", gogen.GoCamelCase(group)))
	for _, oneofField := range fields {
		writeFieldCodeCheck("OneofFieldExists", "msg", oneofField, builder, tabs+1, true)
		builder.WriteString(fmt.Sprintf(indent+"if %s {\n", gogen.GoCamelCase(oneofField.GetName())+"OneofFieldExists"))
		builder.WriteString(fmt.Sprintf(indent+"\texisting%[1]sOneofFields = append(existing%[1]sOneofFields, \"%[2]s\")\n", gogen.GoCamelCase(group), oneofField.GetName()))
		builder.WriteString(indent + "}\n")
	}
	builder.WriteString(fmt.Sprintf(indent+"if len(existing%sOneofFields) %s {\n", gogen.GoCamelCase(group), oneofModePredicate(mode)))
	builder.WriteString(indent + "\t oneofGroupFields := []string{\n")
	for _, oneofField := range fields {
		builder.WriteString(indent + "\t\t\"" + oneofField.GetName() + "\",\n")
	}
	builder.WriteString(indent + "\t}\n")
	builder.WriteString(fmt.Sprintf(
		indent+"\treturn xerrors.Errorf(\"%[1]s one of %%v fields should be specified, but %%d fields specified: %%v\", oneofGroupFields, len(existing%[2]sOneofFields), existing%[2]sOneofFields)\n",
		oneofModeName(mode), gogen.GoCamelCase(group)),
	)
	builder.WriteString(indent + "}\n")
	builder.WriteString(fmt.Sprintf("%s}\n", strings.Repeat("\t", tabs)))

}

func oneofModePredicate(mode options.Oneof_Mode) string {
	switch mode {
	case options.Oneof_MODE_UNSPECIFIED, options.Oneof_AT_MOST_ONE:
		return "> 1"
	case options.Oneof_AT_LEAST_ONE:
		return "< 1"
	case options.Oneof_EXACTLY_ONE:
		return "!= 1"
	}
	return ""
}

func oneofModeName(mode options.Oneof_Mode) string {
	switch mode {
	case options.Oneof_MODE_UNSPECIFIED, options.Oneof_AT_MOST_ONE:
		return "At most"
	case options.Oneof_AT_LEAST_ONE:
		return "At least"
	case options.Oneof_EXACTLY_ONE:
		return "Exactly"
	}
	return ""
}

func writeRequiredOneofCheck(oneof *descriptorpb.OneofDescriptorProto, builder *strings.Builder, tabs int) {
	indent := strings.Repeat("\t", tabs)
	camelName := gogen.GoCamelCase(oneof.GetName())
	builder.WriteString(fmt.Sprintf(indent+"if msg.%s == nil {\n", camelName))
	builder.WriteString(fmt.Sprintf(indent+"\treturn xerrors.New(\"Missing required oneof \\\"%s\\\"\")\n", oneof.GetName()))
	builder.WriteString(indent + "}\n")
}

func writeFieldCodeCheck(resultVarSuffix string, messageVarName string, field *descriptorpb.FieldDescriptorProto, builder *strings.Builder, tabs int, exist bool) {
	indent := strings.Repeat("\t", tabs)

	camelName := gogen.GoCamelCase(field.GetName())
	valIndent := indent
	resEq := ":="
	resultVarSuffix = camelName + resultVarSuffix
	checkedName := fmt.Sprintf("%s.Get%s()", messageVarName, camelName)

	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		resEq = "="
		valIndent += "\t"
		checkedName = messageVarName + "Field"
		builder.WriteString(indent + fmt.Sprintf("%s := false\n", resultVarSuffix))
		builder.WriteString(indent + fmt.Sprintf("for _, %s := range %s.%s {\n", checkedName, messageVarName, camelName))
	}

	var equalOrExclamationMark, mayBeExclamationMark string
	if exist {
		mayBeExclamationMark = "!"
		equalOrExclamationMark = "!"
	} else {
		equalOrExclamationMark = "="
	}

	switch field.GetType() {
	case descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:
		builder.WriteString(valIndent + fmt.Sprintf("%s %s (%s %s= nil)\n", resultVarSuffix, resEq, checkedName, equalOrExclamationMark))
	case descriptorpb.FieldDescriptorProto_TYPE_STRING:
		builder.WriteString(valIndent + fmt.Sprintf("%s %s (%s %s= \"\")\n", resultVarSuffix, resEq, checkedName, equalOrExclamationMark))

	case descriptorpb.FieldDescriptorProto_TYPE_DOUBLE:
		fallthrough
	case descriptorpb.FieldDescriptorProto_TYPE_FLOAT:
		fallthrough
	case descriptorpb.FieldDescriptorProto_TYPE_INT64:
		fallthrough
	case descriptorpb.FieldDescriptorProto_TYPE_INT32:
		fallthrough
	case descriptorpb.FieldDescriptorProto_TYPE_UINT32:
		fallthrough
	case descriptorpb.FieldDescriptorProto_TYPE_SINT32:
		fallthrough
	case descriptorpb.FieldDescriptorProto_TYPE_SINT64:
		fallthrough
	case descriptorpb.FieldDescriptorProto_TYPE_ENUM:
		fallthrough
	case descriptorpb.FieldDescriptorProto_TYPE_UINT64:
		builder.WriteString(valIndent + fmt.Sprintf("%s %s (%s %s= 0)\n", resultVarSuffix, resEq, checkedName, equalOrExclamationMark))
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED64:
	case descriptorpb.FieldDescriptorProto_TYPE_FIXED32:
	case descriptorpb.FieldDescriptorProto_TYPE_BOOL:
		builder.WriteString(valIndent + fmt.Sprintf("%s %s %s%s\n", resultVarSuffix, resEq, mayBeExclamationMark, checkedName))
	case descriptorpb.FieldDescriptorProto_TYPE_GROUP:
	case descriptorpb.FieldDescriptorProto_TYPE_BYTES:
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED32:
	case descriptorpb.FieldDescriptorProto_TYPE_SFIXED64:
	default:
		panic(fmt.Sprintf("Unsupported type %s", field.GetType()))
	}

	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		builder.WriteString(indent + "}\n")
	}
}

func findConflictingFields(msg *descriptorpb.DescriptorProto, conflictingFieldNames []string) (conflictingFields []*descriptorpb.FieldDescriptorProto, err error) {
	// O(N*M), but N is almost always 2, so no need to optimize for now
outer:
	for _, conflictingFieldName := range conflictingFieldNames {
		for _, field := range msg.GetField() {
			if field.GetName() == conflictingFieldName {
				conflictingFields = append(conflictingFields, field)
				continue outer
			}
		}
		return nil, xerrors.Errorf("No such field: %s; fields: %v", conflictingFieldName, msg.GetField())
	}
	return conflictingFields, nil
}

func generateHasPredicate(field *descriptorpb.FieldDescriptorProto) string {
	camelName := gogen.GoCamelCase(field.GetName())
	if field.GetLabel() == descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		return fmt.Sprintf("len(msg.Get%s()) != 0", camelName)
	}
	switch field.GetType() {
	case descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:
		return fmt.Sprintf("msg.Get%s() != nil", camelName)
	case descriptorpb.FieldDescriptorProto_TYPE_STRING:
		return fmt.Sprintf("len(msg.Get%s()) != 0", camelName)
	case descriptorpb.FieldDescriptorProto_TYPE_INT32, descriptorpb.FieldDescriptorProto_TYPE_INT64:
		return fmt.Sprintf("msg.Get%s() != 0", camelName)
	case descriptorpb.FieldDescriptorProto_TYPE_BOOL:
		return fmt.Sprintf("msg.Get%s()", camelName)
	case descriptorpb.FieldDescriptorProto_TYPE_ENUM:
		return fmt.Sprintf("msg.Get%s() != 0", camelName)
	default:
		return "/* UNSUPPORTED FIELD TYPE */"
	}
}

func visibilityToEnvironmentNameCondition(visibility options.Visibility) string {
	switch visibility {
	case options.Visibility_VISIBILITY_IN:
		return fmt.Sprintf("env.Get() == env.%s", "EnvironmentInternal")
	case options.Visibility_VISIBILITY_KZ:
		return fmt.Sprintf("env.Get() == env.%s", "EnvironmentKZ")
	case options.Visibility_VISIBILITY_CLOUD: // old, kz+ru for backwards compatibility
		return fmt.Sprintf("env.Get() == env.%s || env.Get() == env.%s", "EnvironmentRU", "EnvironmentKZ")
	case options.Visibility_VISIBILITY_RU:
		return fmt.Sprintf("env.Get() == env.%s", "EnvironmentRU")
	case options.Visibility_VISIBILITY_DOUBLECLOUD:
		return fmt.Sprintf("env.Get() == env.%s", "EnvironmentAWS")
	case options.Visibility_VISIBILITY_NEBIUS:
		return fmt.Sprintf("env.Get() == env.%s", "EnvironmentNebius")
	default:
		return fmt.Sprintf("env.Get() == env.%s", "/* UNSUPPORTED VISIBILITY */")
	}
}

func writeValidations(field *descriptorpb.FieldDescriptorProto, msg *descriptorpb.DescriptorProto, builder *strings.Builder, tabs int, imports importMap) error {
	customValidations := getCustomValidations(field)
	validationSpec := getFieldInfo(field).GetValidation()
	if len(customValidations) == 0 {
		if err := writeValidation(field, msg, validationSpec, builder, tabs, imports); err != nil {
			return xerrors.Errorf("Cannot write validation for field %s: %w", field.GetName(), err)
		}
		return nil
	}
	indent := strings.Repeat("\t", tabs)
	imports["github.com/doublecloud/transfer/pkg/config/env"] = ""
	for env, validation := range customValidations {

		builder.WriteString(indent + fmt.Sprintf("if env.Get() == env.%s {\n ", env))
		if err := writeValidation(field, msg, validation, builder, tabs+1, imports); err != nil {
			return xerrors.Errorf("Cannot write custom validation for field %s in %s: %w", field.GetName(), env, err)
		}
		builder.WriteString(indent + "} else ")
	}
	builder.WriteString(indent + "{\n")
	if err := writeValidation(field, msg, validationSpec, builder, tabs+1, imports); err != nil {
		return xerrors.Errorf("Cannot write default validation for field %s: %w", field.GetName(), err)
	}
	builder.WriteString(indent + "}\n")
	return nil
}

func getCustomValidations(field *descriptorpb.FieldDescriptorProto) map[string]*options.Validation {
	customValidations := getFieldInfo(field).GetCustomValidation()
	if customValidations == nil {
		return nil
	}
	validationMap := make(map[string]*options.Validation)
	if customValidations.GetInternalCloud() != nil {
		validationMap["EnvironmentInternal"] = customValidations.GetInternalCloud()
	}
	if customValidations.GetCloud() != nil {
		validationMap["EnvironmentRU"] = customValidations.GetCloud()
		validationMap["EnvironmentKZ"] = customValidations.GetCloud()
	}
	if customValidations.GetDoubleCloud() != nil {
		validationMap["EnvironmentAWS"] = customValidations.GetDoubleCloud()
	}
	if customValidations.GetIsrael() != nil {
		validationMap["EnvironmentNebius"] = customValidations.GetIsrael()
	}
	return validationMap
}

func writeValidation(field *descriptorpb.FieldDescriptorProto, msg *descriptorpb.DescriptorProto, validationSpec *options.Validation, builder *strings.Builder, tabs int, imports importMap) error {
	if validationSpec.GetNoValidate() {
		return nil
	}
	if validationSpec != nil {
		if validationSpec.Required {
			writeRequiredCheck(field, builder, tabs)
		}
		if validationSpec.Computed {
			writeComputedCheck(field, builder, tabs)
		}
		if stringRange := validationSpec.GetStringLengthRange(); stringRange != nil {
			if err := writeStringFieldLengthCheck(field, builder, tabs, stringRange.Minimum, stringRange.Maximum); err != nil {
				return xerrors.Errorf("Cannot write string_length_range for field %s: %w", field.GetName(), err)
			}
		}
		if doubleRange := validationSpec.GetDoubleRange(); doubleRange != nil {
			if err := writeDoubleFieldRangeCheck(field, builder, tabs, doubleRange.Minimum, doubleRange.Maximum); err != nil {
				return xerrors.Errorf("Cannot write double_range for field %s: %w", field.GetName(), err)
			}
		}
		if intRange := validationSpec.GetIntRange(); intRange != nil {
			if err := writeIntFieldRangeCheck(field, builder, tabs, intRange.Minimum, intRange.Maximum); err != nil {
				return xerrors.Errorf("Cannot write int_range for field %s: %w", field.GetName(), err)
			}
		}
		if rptRange := validationSpec.GetRepeatedFieldRange(); rptRange != nil {
			if err := writeRepeatableFieldRangeCheck(field, builder, tabs, rptRange.Minimum, rptRange.Maximum); err != nil {
				return xerrors.Errorf("Cannot write repeated_field_length for field %s: %w", field.GetName(), err)
			}
		}
		if mapLength := validationSpec.GetMapLength(); mapLength != nil {
			if err := writeMapLengthCheck(field, builder, tabs, mapLength.Minimum, mapLength.Maximum); err != nil {
				return xerrors.Errorf("Cannot write map_length for field %s: %w", field.GetName(), err)
			}
		}
		if regex := validationSpec.GetRegex(); regex != "" {
			imports["regexp"] = ""
			if err := writeFieldRegexCheck(field, builder, tabs, regex); err != nil {
				return xerrors.Errorf("Cannot write regex check for field %s: %w", field.GetName(), err)
			}
		}
		if keyLength := validationSpec.GetKeyLength(); keyLength != nil {
			if err := writeKeyLengthCheck(field, builder, tabs, keyLength.Minimum, keyLength.Maximum); err != nil {
				return xerrors.Errorf("Cannot write key_length for field %s: %w", field.GetName(), err)
			}
		}
		if valueLength := validationSpec.GetValueLength(); valueLength != nil {
			if err := writeValueLengthCheck(field, builder, tabs, valueLength.Minimum, valueLength.Maximum); err != nil {
				return xerrors.Errorf("Cannot write value_length for field %s: %w", field.GetName(), err)
			}
		}
		if keyRegex := validationSpec.GetKeyRegex(); keyRegex != "" {
			imports["regexp"] = ""
			if err := writeKeyRegexCheck(field, builder, tabs, keyRegex); err != nil {
				return xerrors.Errorf("Cannot write key_regex for field %s: %w", field.GetName(), err)
			}
		}
		if valueRegex := validationSpec.GetValueRegex(); valueRegex != "" {
			imports["regexp"] = ""
			if err := writeValueRegexCheck(field, builder, tabs, valueRegex); err != nil {
				return xerrors.Errorf("Cannot write value_regex for field %s: %w", field.GetName(), err)
			}
		}
	}
	return nil
}

func writeValidateMethod(
	msg *descriptorpb.DescriptorProto,
	msgName string,
	sourceFilePackage string,
	validatableMessages map[fqmn]messageEntry,
	allMessages map[fqmn]messageEntry,
	builder *strings.Builder,
	imports importMap,
	goPackages map[protoPackageName]goPackage,
) error {
	msgName = strings.Join(strings.Split(msgName, "."), "_")
	builder.WriteString(fmt.Sprintf("func (msg *%s) Validate() error {\n", msgName))
	if err := writeLogicalOneofChecks(msg.Field, builder, 1); err != nil {
		return xerrors.Errorf("Cannot write oneofs validation for msg %s: %w", msg.GetName(), err)
	}
	for _, field := range msg.GetField() {

		isHidden := false
		for _, visibility := range getFieldInfo(field).GetAppearance().GetVisibility() {
			isHidden = isHidden || visibility == options.Visibility_VISIBILITY_HIDDEN
		}

		validation := getFieldInfo(field).GetValidation()
		customValidation := getFieldInfo(field).GetCustomValidation()
		shouldValidate := validation != nil || customValidation != nil
		visibilitySet := NewVisibilitySet(getFieldInfo(field).GetAppearance().GetVisibility())

		if isHidden && shouldValidate && visibilitySet.IsVisibleEverywhere() {
			builder.WriteString(fmt.Sprintf("\tif %s {\n", generateHasPredicate(field)))
			if err := writeValidations(field, msg, builder, 3, imports); err != nil {
				return xerrors.Errorf("Cannot write validation for field %s: %w", field.GetName(), err)
			}
			builder.WriteString("\t}\n")
		} else if !visibilitySet.IsVisibleEverywhere() {
			builder.WriteString("\tif (")
			for i, visibility := range visibilitySet.UnsupportedInstallations() {
				if i != 0 {
					builder.WriteString(" || ")
				}
				imports["github.com/doublecloud/transfer/pkg/config/env"] = ""
				builder.WriteString(visibilityToEnvironmentNameCondition(visibility))
			}
			builder.WriteString(") {\n")
			builder.WriteString(fmt.Sprintf("\t\tif %s {\n", generateHasPredicate(field)))
			builder.WriteString(fmt.Sprintf("\t\t\treturn xerrors.New(\"%s: unsupported feature\")\n", field.GetName()))
			builder.WriteString("\t\t}\n")

			if shouldValidate {
				builder.WriteString("\t} else {\n")
				if err := writeValidations(field, msg, builder, 2, imports); err != nil {
					return xerrors.Errorf("Cannot write validation for field %s: %w", field.GetName(), err)
				}
			}
			builder.WriteString("\t}\n")
		} else if shouldValidate {
			if err := writeValidations(field, msg, builder, 1, imports); err != nil {
				return xerrors.Errorf("Cannot write validation for field %s: %w", field.GetName(), err)
			}
		}

		if validation.GetNoValidate() {
			continue
		}

		switch field.GetType() {
		case descriptorpb.FieldDescriptorProto_TYPE_MESSAGE:
			if getFieldInfo(field).GetValidation().GetNoValidateContainingFields() {
				break
			}
			if _, ok := validatableMessages[fqmn(field.GetTypeName())]; !ok {
				continue
			}
			writeMessageFieldCheck(field, builder)
		case descriptorpb.FieldDescriptorProto_TYPE_ENUM:
			writeEnumFieldCheck(field, sourceFilePackage, allMessages, builder, imports, goPackages)
		}
	}

	for _, oneof := range msg.GetOneofDecl() {
		validationSpec := getOneofInfo(oneof).GetValidation()
		if validationSpec == nil {
			continue
		}

		if validationSpec.Required {
			writeRequiredOneofCheck(oneof, builder, 1)
		}
	}

	builder.WriteString("\treturn nil\n")
	builder.WriteString("}\n")
	return nil
}

func writeValidateMethods(
	sourceFile *descriptorpb.FileDescriptorProto,
	allFiles []*descriptorpb.FileDescriptorProto,
	bodyBuilder *strings.Builder,
	imports importMap,
) (hasValidatableMessages bool, err error) {
	allValidatableMessages, allMessages, goPackages, err := listMessages(allFiles, bodyBuilder)
	if err != nil {
		return false, xerrors.Errorf("cannot list messages: %w", err)
	}
	var msgToBeCheck []struct {
		descriptor *descriptorpb.DescriptorProto
		prefix     string
	}
	for _, msg := range sourceFile.GetMessageType() {
		msgToBeCheck = append(msgToBeCheck, struct {
			descriptor *descriptorpb.DescriptorProto
			prefix     string
		}{descriptor: msg, prefix: ""})
	}
	i := 0
	for {
		if i == len(msgToBeCheck) {
			break
		}
		msg := msgToBeCheck[i].descriptor
		for _, nestedMsg := range msg.GetNestedType() {
			skip := false
			// Skip type if it's map entry or key type
			for _, field := range msg.Field {
				fieldName := strings.ToUpper(field.GetName()[:1]) + field.GetName()[1:]
				if strings.Compare(nestedMsg.GetName(), fieldName+"Entry") == 0 {
					skip = true
				}
				if strings.Compare(nestedMsg.GetName(), fieldName+"Key") == 0 {
					skip = true
				}
			}
			if skip {
				continue
			}
			msgToBeCheck = append(msgToBeCheck, struct {
				descriptor *descriptorpb.DescriptorProto
				prefix     string
			}{descriptor: nestedMsg, prefix: msg.GetName() + "."})
		}
		validatable, err := checkMessageAndWriteValidation(
			sourceFile,
			allValidatableMessages,
			msg,
			msgToBeCheck[i].prefix+msg.GetName(),
			bodyBuilder,
			allMessages,
			imports,
			goPackages,
		)
		if err != nil {
			return false, err
		}
		i++
		hasValidatableMessages = hasValidatableMessages || validatable
	}
	return hasValidatableMessages, nil
}

func checkMessageAndWriteValidation(
	sourceFile *descriptorpb.FileDescriptorProto,
	allValidatableMessages map[fqmn]messageEntry,
	msg *descriptorpb.DescriptorProto,
	msgName string,
	bodyBuilder *strings.Builder,
	allMessages map[fqmn]messageEntry,
	imports importMap,
	goPackages map[protoPackageName]goPackage,
) (bool, error) {
	if _, isValidatable := allValidatableMessages[messageKey{
		packageName: sourceFile.GetPackage(),
		messageName: msgName,
	}.fqmn()]; !isValidatable {
		return false, nil
	}

	bodyBuilder.WriteString("\n")
	if err := writeValidateMethod(
		msg,
		msgName,
		"."+sourceFile.GetPackage(),
		allValidatableMessages,
		allMessages,
		bodyBuilder,
		imports,
		goPackages,
	); err != nil {
		return false, xerrors.Errorf("message %s: %w", msg, err)
	}
	return true, nil
}

type serviceTemplateArguments struct {
	CamelCaseName string
	Methods       []methodTemplateArguments
	imports       importMap
}

func (a *serviceTemplateArguments) Package(packagePath string) string {
	baseName := filepath.Base(packagePath)
	a.imports[importPath(packagePath)] = importAlias(baseName)
	return baseName
}

func (a *serviceTemplateArguments) isValidatable() bool {
	for _, method := range a.Methods {
		if method.SkipValidation {
			continue
		}
		if method.RequestType.SkipValidation {
			continue
		}
		return true
	}
	return false
}

type methodTemplateArguments struct {
	CamelCaseName  string
	ProtoName      string
	RequestType    requestType
	ReturnTypeName string
	SkipValidation bool
}

type requestType struct {
	Name           string
	SkipValidation bool
}

func makeRequestTypeTemplateArgument(
	sourceFile *descriptorpb.FileDescriptorProto,
	allFiles []*descriptorpb.FileDescriptorProto,
	allMessages map[fqmn]messageEntry,
	allValidatableMessages map[fqmn]messageEntry,
	method *descriptorpb.MethodDescriptorProto,
	imports importMap,
	goPackages map[protoPackageName]goPackage,
) requestType {
	_, isValidatable := allValidatableMessages[fqmn(method.GetInputType())]
	return requestType{
		Name:           goTypeName(method.GetInputType(), sourceFile.GetPackage(), allMessages, imports, goPackages),
		SkipValidation: !isValidatable,
	}
}

func makeMethodsTemplateArguments(
	sourceFile *descriptorpb.FileDescriptorProto,
	allFiles []*descriptorpb.FileDescriptorProto,
	allMessages map[fqmn]messageEntry,
	allValidatableMessages map[fqmn]messageEntry,
	methods []*descriptorpb.MethodDescriptorProto,
	imports importMap,
	goPackages map[protoPackageName]goPackage,
) (templateArguments []methodTemplateArguments) {
	for _, method := range methods {
		if method.GetName() == "StreamLogs" {
			continue
		}
		templateArguments = append(templateArguments, methodTemplateArguments{
			CamelCaseName:  gogen.GoCamelCase(method.GetName()),
			ProtoName:      method.GetName(),
			RequestType:    makeRequestTypeTemplateArgument(sourceFile, allFiles, allMessages, allValidatableMessages, method, imports, goPackages),
			ReturnTypeName: goTypeName(method.GetOutputType(), sourceFile.GetPackage(), allMessages, imports, goPackages),
			SkipValidation: getMethodInfo(method).GetSkipValidation(),
		})
	}
	return templateArguments
}

func writeValidatingServiceWrappers(
	sourceFile *descriptorpb.FileDescriptorProto,
	allFiles []*descriptorpb.FileDescriptorProto,
	imports importMap,
	bodyBuilder *strings.Builder,
) (hasValidatableServices bool, err error) {
	allValidatableMessages, allMessages, goPackages, err := listMessages(allFiles, bodyBuilder)
	if err != nil {
		return false, xerrors.Errorf("cannot list messages: %w", err)
	}
	for _, service := range sourceFile.GetService() {
		serviceTemplateArguments := &serviceTemplateArguments{
			CamelCaseName: gogen.GoCamelCase(service.GetName()),
			Methods:       makeMethodsTemplateArguments(sourceFile, allFiles, allMessages, allValidatableMessages, service.GetMethod(), imports, goPackages),
			imports:       imports,
		}
		if serviceTemplateArguments.isValidatable() {
			hasValidatableServices = true
		}
		if err := serviceWrapperTemplate.Execute(bodyBuilder, serviceTemplateArguments); err != nil {
			return false, xerrors.Errorf("cannot execute template for service %s: %w", service.GetName(), err)
		}
		imports["context"] = ""
	}
	return hasValidatableServices, nil
}

func getPkgName(goPackage string) string {
	pkgName := filepath.Base(goPackage)
	// package may be like '.../cloud/priv/mdb/mysql/v1;mysql'
	i := strings.Index(pkgName, ";")
	if i != -1 {
		return pkgName[i+1:]
	} else {
		return pkgName
	}
}

func generate(
	sourceFile *descriptorpb.FileDescriptorProto,
	allFiles []*descriptorpb.FileDescriptorProto,
) (fileContent string, err error) {
	pkgName := getPkgName(*sourceFile.Options.GoPackage)
	bodyBuilder := strings.Builder{}
	imports := importMap{}

	hasValidatableMessages, err := writeValidateMethods(sourceFile, allFiles, &bodyBuilder, imports)
	if err != nil {
		return "", xerrors.Errorf("Cannot write Validate() methods: %w", err)
	}
	hasValidatableServices, err := writeValidatingServiceWrappers(sourceFile, allFiles, imports, &bodyBuilder)
	if err != nil {
		return "", xerrors.Errorf("Cannot write validating service wrappers: %w", err)
	}
	if hasValidatableMessages || hasValidatableServices {
		imports["github.com/doublecloud/transfer/library/go/core/xerrors"] = ""
	}

	sortedImports := make([]string, 0, len(imports))
	for imp := range imports {
		sortedImports = append(sortedImports, string(imp))
	}
	sort.Strings(sortedImports)

	headerBuilder := strings.Builder{}
	headerBuilder.WriteString(fmt.Sprintf("package %s\n", pkgName))
	headerBuilder.WriteString("\n")
	headerBuilder.WriteString("import (\n")
	for _, imp := range sortedImports {
		headerBuilder.WriteString(fmt.Sprintf("\t\"%s\"\n", imp))
	}
	headerBuilder.WriteString(")\n")
	return headerBuilder.String() + bodyBuilder.String(), nil
}

func main() {
	gogen.ProtoPluginMain("genvalidate", generate, ".vld.go")
}
