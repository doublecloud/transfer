package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/doublecloud/transfer/cloud/dataplatform/protogen/pkg/gogen"
	"google.golang.org/protobuf/types/descriptorpb"
)

func generateOneofsForMessage(messageDescriptor *descriptorpb.DescriptorProto, parentStructName string, bodyBuilder *strings.Builder) {
	var currentStructName string
	if parentStructName == "" {
		currentStructName = messageDescriptor.GetName()
	} else {
		currentStructName = fmt.Sprintf("%s_%s", parentStructName, messageDescriptor.GetName())
	}

	for _, oneofDescriptor := range messageDescriptor.GetOneofDecl() {
		oneofCamelName := gogen.GoCamelCase(oneofDescriptor.GetName())
		privateTypeName := fmt.Sprintf("is%s_%s", currentStructName, oneofCamelName)
		publicTypeName := fmt.Sprintf("Is%s_%s", currentStructName, oneofCamelName)

		bodyBuilder.WriteString("\n")
		bodyBuilder.WriteString(fmt.Sprintf("//go-sumtype:decl %s\n", publicTypeName))
		bodyBuilder.WriteString(fmt.Sprintf("//go-sumtype:decl %s\n", privateTypeName))
		bodyBuilder.WriteString(fmt.Sprintf("type %s interface {\n", publicTypeName))
		bodyBuilder.WriteString(fmt.Sprintf("\t%s\n", privateTypeName))
		bodyBuilder.WriteString("}\n")
	}

	for _, nestedMessageDescriptor := range messageDescriptor.GetNestedType() {
		generateOneofsForMessage(nestedMessageDescriptor, currentStructName, bodyBuilder)
	}
}

func generate(sourceFile *descriptorpb.FileDescriptorProto, allFiles []*descriptorpb.FileDescriptorProto) (fileContent string, err error) {
	bodyBuilder := strings.Builder{}
	for _, messageDescriptor := range sourceFile.GetMessageType() {
		generateOneofsForMessage(messageDescriptor, "", &bodyBuilder)
	}

	goPackage := filepath.Base(sourceFile.GetOptions().GetGoPackage())
	semicolonSplitComponents := strings.Split(goPackage, ";")
	if len(semicolonSplitComponents) > 1 {
		goPackage = semicolonSplitComponents[len(semicolonSplitComponents)-1]
	}
	body := bodyBuilder.String()
	if len(body) == 0 {
		return fmt.Sprintf("package %s", goPackage), nil
	}

	headerBuilder := strings.Builder{}
	headerBuilder.WriteString(fmt.Sprintf("package %s\n", goPackage))
	header := headerBuilder.String()

	return header + body, nil
}

func main() {
	gogen.ProtoPluginMain("genoneof", generate, ".oneof.go")
}
