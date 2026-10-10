package agent

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

const eksTokenPrefix = "k8s-aws-v1."

// EKS's IAM authenticator requires X-Amz-Expires to be present in the
// presigned STS URL. STS itself ignores this value for GetCallerIdentity, but
// the authenticator validates it before making the STS call. Match the
// upstream aws-iam-authenticator token generator's legacy value.
const eksPresignExpiresSeconds = "60"

// BootstrapTokenIssuer mints the short-lived bearer credential placed in a
// kubelet kubeconfig. Kubernetes distributions normally use a bootstrap-token
// Secret initially; managed EKS hybrid nodes use renewable IAM-authenticator
// tokens for ongoing kubelet authentication.
type BootstrapTokenIssuer interface {
	Issue(context.Context, string) (string, error)
}

type assumeRoleAPI interface {
	AssumeRole(context.Context, *sts.AssumeRoleInput, ...func(*sts.Options)) (*sts.AssumeRoleOutput, error)
}

type eksTokenIssuer struct {
	clusterName string
	roleARN     string
	config      aws.Config
	sts         assumeRoleAPI
}

// NewEKSTokenIssuer returns an issuer that uses the connector Pod's ambient
// AWS identity (normally IRSA) only to assume a zero-permission bootstrap role.
// The role session name is the exact burst Node name, which EKS projects into
// the hybrid node identity through the access entry configured at cluster
// install time. No AWS credential is returned to the burst VM.
func NewEKSTokenIssuer(ctx context.Context, clusterName, roleARN, region string) (BootstrapTokenIssuer, error) {
	clusterName = strings.TrimSpace(clusterName)
	roleARN = strings.TrimSpace(roleARN)
	region = strings.TrimSpace(region)
	if clusterName == "" || roleARN == "" || region == "" {
		return nil, fmt.Errorf("EKS bootstrap requires cluster name, role ARN, and region")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config for EKS bootstrap: %w", err)
	}
	return &eksTokenIssuer{
		clusterName: clusterName,
		roleARN:     roleARN,
		config:      cfg,
		sts:         sts.NewFromConfig(cfg),
	}, nil
}

func (i *eksTokenIssuer) Issue(ctx context.Context, nodeName string) (string, error) {
	if _, ok := burstIDForNodeName(nodeName); !ok {
		return "", fmt.Errorf("invalid EKS bootstrap node name %q", nodeName)
	}
	out, err := i.sts.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(i.roleARN),
		RoleSessionName: aws.String(nodeName),
		DurationSeconds: aws.Int32(900),
	})
	if err != nil {
		return "", fmt.Errorf("assume EKS bootstrap role: %w", err)
	}
	if out.Credentials == nil || out.Credentials.AccessKeyId == nil || out.Credentials.SecretAccessKey == nil || out.Credentials.SessionToken == nil {
		return "", fmt.Errorf("assume EKS bootstrap role returned incomplete credentials")
	}

	cfg := i.config
	cfg.Credentials = credentials.NewStaticCredentialsProvider(
		aws.ToString(out.Credentials.AccessKeyId),
		aws.ToString(out.Credentials.SecretAccessKey),
		aws.ToString(out.Credentials.SessionToken),
	)
	client := sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.APIOptions = append(o.APIOptions, addEKSClusterIDHeader(i.clusterName))
	})
	presigned, err := sts.NewPresignClient(client).PresignGetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("presign EKS identity: %w", err)
	}
	if presigned.URL == "" {
		return "", fmt.Errorf("presign EKS identity returned an empty URL")
	}
	return eksTokenPrefix + base64.RawURLEncoding.EncodeToString([]byte(presigned.URL)), nil
}

type eksClusterIDHeader struct{ clusterName string }

func (m eksClusterIDHeader) ID() string { return "YScaleEKSClusterIDHeader" }

func (m eksClusterIDHeader) HandleBuild(ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler) (
	out middleware.BuildOutput, metadata middleware.Metadata, err error,
) {
	req, ok := in.Request.(*smithyhttp.Request)
	if !ok {
		return out, metadata, fmt.Errorf("unexpected STS request type %T", in.Request)
	}
	req.Header.Set("x-k8s-aws-id", m.clusterName)
	query := req.URL.Query()
	query.Set("X-Amz-Expires", eksPresignExpiresSeconds)
	req.URL.RawQuery = query.Encode()
	return next.HandleBuild(ctx, in)
}

func addEKSClusterIDHeader(clusterName string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Build.Add(eksClusterIDHeader{clusterName: clusterName}, middleware.Before)
	}
}

var _ BootstrapTokenIssuer = (*eksTokenIssuer)(nil)
