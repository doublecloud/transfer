package iceberg

import (
	"os"

	go_iceberg "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
)

func SourceRecipe() (*IcebergSource, error) {
	if _, ok := os.LookupEnv("AWS_S3_ENDPOINT"); ok {
		return &IcebergSource{
			Properties: go_iceberg.Properties{
				io.S3Region:          "us-east-1",
				io.S3AccessKeyID:     os.Getenv("S3_USER"),
				io.S3SecretAccessKey: "password",
				"type":               "rest",
				"url":                os.Getenv("CATALOG_URL"),
			},
			CatalogType: "rest",
			CatalogURI:  os.Getenv("CATALOG_URL"),
			Schema:      "default",
		}, nil
	}
	return nil, xerrors.New("recipe not supported")
}
