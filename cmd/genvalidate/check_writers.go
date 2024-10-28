package main

import (
	"fmt"
	"strings"
)

type CheckWriter[T any] interface {
	Write(varName string, params *T, tabs int, errorExpr string) string
}

type IntCheckWriter struct {
}

type OptionalIntCheckWriter struct {
}

type IntCheckParams struct {
	Min int64
	Max int64
}

func (intWriter *IntCheckWriter) Write(varName string, params *IntCheckParams, tabs int, errorExpr string) string {
	result := []string{
		fmt.Sprintf("if %[1]s != 0 && (%[1]s>%[2]d || %[1]s<%[3]d) {", varName, params.Max, params.Min),
		fmt.Sprintf("\treturn %s", fmt.Sprintf(errorExpr, MakeIntErrorStr(params))),
		"}\n",
	}
	for i := range result {
		result[i] = strings.Repeat("\t", tabs) + result[i]
	}
	return strings.Join(result, "\n")
}

func (intWriter *OptionalIntCheckWriter) Write(varName string, params *IntCheckParams, tabs int, errorExpr string) string {
	result := []string{
		fmt.Sprintf("if %[1]s != nil && (%[1]s.Value>%[2]d || %[1]s.Value<%[3]d) {", varName, params.Max, params.Min),
		fmt.Sprintf("\treturn %s", fmt.Sprintf(errorExpr, MakeIntErrorStr(params))),
		"}\n",
	}
	for i := range result {
		result[i] = strings.Repeat("\t", tabs) + result[i]
	}
	return strings.Join(result, "\n")
}

func MakeIntErrorStr(params *IntCheckParams) string {
	return fmt.Sprintf("is outside allowed range [%d, %d]", params.Min, params.Max)
}

type FloatCheckWriter struct {
}

type FloatCheckParams struct {
	Min float64
	Max float64
}

func (intWriter *FloatCheckWriter) Write(varName string, params *FloatCheckParams, tabs int, errorExpr string) string {
	result := []string{
		fmt.Sprintf("if %[1]s != 0 && (%[1]s>%[2]f || %[1]s<%[3]f) {", varName, params.Max, params.Min),
		fmt.Sprintf("\treturn %s", fmt.Sprintf(errorExpr, intWriter.MakeErrorStr(params))),
		"}\n",
	}
	for i := range result {
		result[i] = strings.Repeat("\t", tabs) + result[i]
	}
	return strings.Join(result, "\n")
}

func (intWriter *FloatCheckWriter) MakeErrorStr(params *FloatCheckParams) string {
	return fmt.Sprintf("is outside allowed range [%f, %f]", params.Min, params.Max)
}
