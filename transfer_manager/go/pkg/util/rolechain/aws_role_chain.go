package rolechain

import (
	"github.com/aws/aws-sdk-go/aws"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

func newSession(
	creds *aws_credentials.Credentials,
) *session.Session {
	return session.Must(session.NewSession(
		aws.NewConfig().WithCredentials(creds),
	))
}

func singleStep(
	ses *session.Session,
	roleArn string,
) *session.Session {
	creds := stscreds.NewCredentials(ses, roleArn)
	return newSession(creds)
}

// NewSession allows to create Session using multiple role assumptions.
// For example: RoleA assumes RoleB, RoleB assumes RoleC.
func NewSession(
	ses *session.Session,
	roles ...string,
) *session.Session {
	for _, role := range roles {
		ses = singleStep(ses, role)
	}
	return ses
}
