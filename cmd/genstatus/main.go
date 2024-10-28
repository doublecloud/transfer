package main

import (
	_ "embed"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"github.com/doublecloud/transfer/cloud/dataplatform/protogen/pkg/gogen"
	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"google.golang.org/protobuf/types/descriptorpb"
)

var (
	//go:embed header.tmpl
	headerTemplateData string

	//go:embed body.tmpl
	bodyTemplateData string

	headerTemplate = template.Must(template.New("header.tmpl").Parse(headerTemplateData))
	bodyTemplate   = template.Must(template.New("body.tmpl").Parse(bodyTemplateData))
)

type File struct {
	PackageName string
	Imports     map[string]string
	Services    []*Service

	typeResolver *gogen.TypeResolver
}

func (f *File) SortedImports() []string {
	sortedImports := make([]string, 0, len(f.Imports))
	for _, imp := range f.Imports {
		sortedImports = append(sortedImports, imp)
	}
	sort.Strings(sortedImports)
	return sortedImports
}

func (f *File) Package(path string) string {
	base := filepath.Base(path)
	f.Imports[base] = path
	return base
}

func NewFile(typeResolver *gogen.TypeResolver, protoFile *descriptorpb.FileDescriptorProto) *File {
	file := new(File)
	var services []*Service
	for _, protoService := range protoFile.Service {
		services = append(services, NewService(file, protoService))
	}
	packageName := filepath.Base(*protoFile.Options.GoPackage)
	if strings.Contains(packageName, ";") {
		split := strings.Split(packageName, ";")
		packageName = split[len(split)-1]
	}

	*file = File{
		PackageName:  packageName,
		Imports:      make(map[string]string), // will be populated later
		Services:     services,
		typeResolver: typeResolver,
	}
	return file
}

type Service struct {
	CamelCaseName string
	Methods       []*Method
}

func lastComponent(fullProtoTypeName string) string {
	components := strings.Split(fullProtoTypeName, ".")
	return components[len(components)-1]
}

func NewService(file *File, protoService *descriptorpb.ServiceDescriptorProto) *Service {
	var methods []*Method
	for _, protoMethod := range protoService.Method {
		methods = append(methods, &Method{
			CamelCaseName: protoMethod.GetName(),
			requestFqtn:   *protoMethod.InputType,
			resultFqtn:    *protoMethod.OutputType,
			file:          file,
		})
	}
	return &Service{
		CamelCaseName: protoService.GetName(),
		Methods:       methods,
	}
}

type Method struct {
	file          *File
	CamelCaseName string
	requestFqtn   string
	resultFqtn    string
}

func (m *Method) RequestTypeName() string {
	return m.file.typeResolver.GoTypeName(m.requestFqtn, m.file.Imports)
}

func (m *Method) ReturnTypeName() string {
	return m.file.typeResolver.GoTypeName(m.resultFqtn, m.file.Imports)
}

func generate(
	sourceFile *descriptorpb.FileDescriptorProto,
	allFiles []*descriptorpb.FileDescriptorProto,
) (fileContent string, err error) {
	typeResolver, err := gogen.NewTypeResolver(sourceFile, allFiles)
	if err != nil {
		return "", xerrors.Errorf("cannot create type resolver: %w", err)
	}
	file := NewFile(typeResolver, sourceFile)

	bodyBuilder := strings.Builder{}
	if err := bodyTemplate.Execute(&bodyBuilder, file); err != nil {
		return "", xerrors.Errorf("cannot execute body template: %w", err)
	}

	headerBuilder := strings.Builder{}
	if err := headerTemplate.Execute(&headerBuilder, file); err != nil {
		return "", xerrors.Errorf("cannot execute header template: %w", err)
	}

	return headerBuilder.String() + bodyBuilder.String(), nil
}

func main() {
	gogen.ProtoPluginMain("genstatus", generate, ".status.go")
}
