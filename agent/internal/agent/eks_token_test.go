package agent

import (
	"context"
	"encoding/base64"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
)

type recordingAssumeRole struct {
	input *sts.AssumeRoleInput
	calls int
}

func (f *recordingAssumeRole) AssumeRole(_ context.Context, in *sts.AssumeRoleInput, _ ...func(*sts.Options)) (*sts.AssumeRoleOutput, error) {
	f.calls++
	f.input = in
	return &sts.AssumeRoleOutput{Credentials: &ststypes.Credentials{
		AccessKeyId:     aws.String("AKIDEXAMPLE"),
		SecretAccessKey: aws.String("secret-example"),
		SessionToken:    aws.String("session-example"),
		Expiration:      aws.Time(time.Now().Add(15 * time.Minute)),
	}}, nil
}

func TestEKSTokenIssuerBindsRoleSessionAndSignedCluster(t *testing.T) {
	assumer := &recordingAssumeRole{}
	issuer := &eksTokenIssuer{
		clusterName: "cluster-a",
		roleARN:     "arn:aws:iam::123456789012:role/yscale-bootstrap",
		config: aws.Config{
			Region:      "us-east-1",
			Credentials: credentials.NewStaticCredentialsProvider("unused", "unused", ""),
		},
		sts: assumer,
	}
	nodeName := "ys-burst-012345abcdef"
	token, err := issuer.Issue(context.Background(), nodeName)
	if err != nil {
		t.Fatal(err)
	}
	if assumer.calls != 1 {
		t.Fatalf("AssumeRole calls = %d, want 1", assumer.calls)
	}
	if got := aws.ToString(assumer.input.RoleSessionName); got != nodeName {
		t.Fatalf("role session = %q, want %q", got, nodeName)
	}
	if got := aws.ToString(assumer.input.RoleArn); got != issuer.roleARN {
		t.Fatalf("role ARN = %q, want %q", got, issuer.roleARN)
	}
	if got := aws.ToInt32(assumer.input.DurationSeconds); got != 900 {
		t.Fatalf("duration = %d, want 900", got)
	}

	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(token, eksTokenPrefix))
	if err != nil {
		t.Fatalf("decode token: %v", err)
	}
	signedURL, err := url.Parse(string(raw))
	if err != nil {
		t.Fatalf("parse signed URL: %v", err)
	}
	if got := signedURL.Query().Get("Action"); got != "GetCallerIdentity" {
		t.Fatalf("action = %q, want GetCallerIdentity", got)
	}
	if got := signedURL.Query().Get("X-Amz-SignedHeaders"); !strings.Contains(got, "x-k8s-aws-id") {
		t.Fatalf("signed headers = %q, missing x-k8s-aws-id", got)
	}
	if got := signedURL.Query().Get("X-Amz-Expires"); got != eksPresignExpiresSeconds {
		t.Fatalf("X-Amz-Expires = %q, want %q", got, eksPresignExpiresSeconds)
	}
}

func TestEKSTokenIssuerRejectsNonCanonicalNodeBeforeAWS(t *testing.T) {
	assumer := &recordingAssumeRole{}
	issuer := &eksTokenIssuer{sts: assumer}
	if _, err := issuer.Issue(context.Background(), "ys-burst-not-hex"); err == nil {
		t.Fatal("Issue accepted non-canonical burst node name")
	}
	if assumer.calls != 0 {
		t.Fatalf("AssumeRole calls = %d, want 0", assumer.calls)
	}
}
