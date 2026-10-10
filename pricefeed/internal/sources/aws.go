package sources

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	pricingtypes "github.com/aws/aws-sdk-go-v2/service/pricing/types"

	"github.com/yscale-sh/yscale/pricefeed/internal/feed"
)

// awsRegions is the region set the source prices. Kept small — each
// region is a separate GetProducts paginate + spot-history call.
var awsRegions = []string{"us-east-1", "us-west-2", "eu-west-1"}

// awsFamilies maps an EC2 instance family to a yscale abstract kind.
// Only these families are emitted — the allowlist keeps the feed
// focused and the spot API response bounded.
var awsFamilies = map[string]string{
	"p5":   "h100",
	"p4d":  "a100",
	"p4de": "a100",
	"g6e":  "l4",
	"g6":   "l4",
	"g5":   "a10g",
	"g4dn": "t4",
	"m7i":  "cpu",
	"c7i":  "cpu",
	"r7i":  "cpu",
}

// onDemandTTL bounds how long a GetProducts result is reused. On-demand
// prices change ~monthly, so there's no need to re-pull them every poll
// — only spot is refetched each Fetch.
const onDemandTTL = 6 * time.Hour

// AWS fetches EC2 on-demand prices (Price List GetProducts) and spot
// prices (DescribeSpotPriceHistory). Reads standard AWS_* env creds via
// the default credential chain.
//
// On-demand uses the lightweight `pricing` SDK. Spot deliberately does
// NOT use the `service/ec2` SDK — that package is enormous and OOMs the
// Go compiler on constrained machines — so the one EC2 call we need is
// a hand-signed (SigV4) request to the EC2 query API.
type AWS struct {
	odCache     []feed.Offering // cached on-demand offerings
	odFetchedAt time.Time
}

// NewAWS constructs the AWS source.
func NewAWS() *AWS { return &AWS{} }

func (s *AWS) Name() string { return "aws" }

// Interval — spot prices drift over hours (since AWS's 2017 model
// change they're no longer a per-second auction), so 15 min has wide
// margin; on-demand is cached for onDemandTTL between polls.
func (s *AWS) Interval() time.Duration { return 15 * time.Minute }

// Fetch returns on-demand (cached up to onDemandTTL) plus fresh spot
// offerings across awsRegions.
func (s *AWS) Fetch(ctx context.Context) ([]feed.Offering, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("aws: load config: %w", err)
	}

	onDemand := s.odCache
	if len(onDemand) == 0 || time.Since(s.odFetchedAt) > onDemandTTL {
		onDemand, err = fetchAWSOnDemand(ctx, cfg)
		if err != nil {
			return nil, fmt.Errorf("aws: on-demand: %w", err)
		}
		s.odCache, s.odFetchedAt = onDemand, time.Now()
	}

	// Specs (vCPU/mem/GPU) only come back on the on-demand product;
	// reuse them to enrich the spot offerings for the same instance type.
	specs := make(map[string]feed.Offering, len(onDemand))
	for _, o := range onDemand {
		if _, ok := specs[o.SKU]; !ok {
			specs[o.SKU] = o
		}
	}

	spot, err := fetchAWSSpot(ctx, cfg, specs)
	if err != nil {
		return nil, fmt.Errorf("aws: spot: %w", err)
	}

	out := make([]feed.Offering, 0, len(onDemand)+len(spot))
	out = append(out, onDemand...)
	out = append(out, spot...)
	return out, nil
}

// --- on-demand (Price List GetProducts) ----------------------------------

func fetchAWSOnDemand(ctx context.Context, cfg aws.Config) ([]feed.Offering, error) {
	pc := pricing.NewFromConfig(cfg)
	now := time.Now().UTC()
	var out []feed.Offering
	for _, region := range awsRegions {
		in := &pricing.GetProductsInput{
			ServiceCode: aws.String("AmazonEC2"),
			Filters: []pricingtypes.Filter{
				termMatch("regionCode", region),
				termMatch("operatingSystem", "Linux"),
				termMatch("tenancy", "Shared"),
				termMatch("preInstalledSw", "NA"),
				termMatch("capacitystatus", "Used"),
			},
		}
		p := pricing.NewGetProductsPaginator(pc, in)
		for p.HasMorePages() {
			page, err := p.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, raw := range page.PriceList {
				if o, ok := parseAWSProduct(raw, region, now); ok {
					out = append(out, o)
				}
			}
		}
	}
	return out, nil
}

func termMatch(field, value string) pricingtypes.Filter {
	return pricingtypes.Filter{
		Type:  pricingtypes.FilterTypeTermMatch,
		Field: aws.String(field),
		Value: aws.String(value),
	}
}

// awsProduct is the subset of a Price List item JSON we read.
type awsProduct struct {
	Product struct {
		Attributes struct {
			InstanceType string `json:"instanceType"`
			VCPU         string `json:"vcpu"`
			Memory       string `json:"memory"`
			GPU          string `json:"gpu"`
		} `json:"attributes"`
	} `json:"product"`
	Terms struct {
		OnDemand map[string]struct {
			PriceDimensions map[string]struct {
				PricePerUnit struct {
					USD string `json:"USD"`
				} `json:"pricePerUnit"`
			} `json:"priceDimensions"`
		} `json:"OnDemand"`
	} `json:"terms"`
}

func parseAWSProduct(raw, region string, now time.Time) (feed.Offering, bool) {
	var p awsProduct
	if err := json.Unmarshal([]byte(raw), &p); err != nil {
		return feed.Offering{}, false
	}
	a := p.Product.Attributes
	kind, ok := awsKind(a.InstanceType)
	if !ok {
		return feed.Offering{}, false
	}
	usd := firstOnDemandUSD(p)
	if usd == 0 {
		return feed.Offering{}, false
	}
	gpu, _ := strconv.Atoi(a.GPU)
	vcpu, _ := strconv.Atoi(a.VCPU)
	return feed.Offering{
		Provider:    "aws",
		SKU:         a.InstanceType,
		Kind:        kind,
		Region:      region,
		GPU:         gpu > 0,
		GPUCount:    gpu,
		VCPU:        vcpu,
		MemoryMB:    parseGiB(a.Memory),
		Reliability: feed.OnDemand,
		USDPerHour:  usd,
		Available:   true,
		ObservedAt:  now,
	}, true
}

func firstOnDemandUSD(p awsProduct) float64 {
	for _, term := range p.Terms.OnDemand {
		for _, dim := range term.PriceDimensions {
			if v, err := strconv.ParseFloat(dim.PricePerUnit.USD, 64); err == nil {
				return v
			}
		}
	}
	return 0
}

// --- spot (EC2 DescribeSpotPriceHistory, hand-signed) --------------------

const ec2APIVersion = "2016-11-15"

// ec2SpotResponse / ec2SpotItem mirror the EC2 query-API XML response.
type ec2SpotResponse struct {
	XMLName   xml.Name      `xml:"DescribeSpotPriceHistoryResponse"`
	History   []ec2SpotItem `xml:"spotPriceHistorySet>item"`
	NextToken string        `xml:"nextToken"`
}

type ec2SpotItem struct {
	InstanceType     string `xml:"instanceType"`
	AvailabilityZone string `xml:"availabilityZone"`
	SpotPrice        string `xml:"spotPrice"`
	Timestamp        string `xml:"timestamp"` // ISO8601 — sorts lexically
}

func fetchAWSSpot(ctx context.Context, cfg aws.Config, specs map[string]feed.Offering) ([]feed.Offering, error) {
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, fmt.Errorf("retrieve creds: %w", err)
	}
	signer := v4.NewSigner()
	client := &http.Client{Timeout: 30 * time.Second}
	now := time.Now().UTC()

	var out []feed.Offering
	for _, region := range awsRegions {
		// Keep the latest price per (instanceType, AZ).
		latest := map[string]ec2SpotItem{}
		token := ""
		for {
			page, err := ec2SpotPage(ctx, client, signer, creds, region, token)
			if err != nil {
				return nil, err
			}
			for _, it := range page.History {
				key := it.InstanceType + "|" + it.AvailabilityZone
				if cur, ok := latest[key]; !ok || it.Timestamp > cur.Timestamp {
					latest[key] = it
				}
			}
			if page.NextToken == "" {
				break
			}
			token = page.NextToken
		}
		for _, it := range latest {
			kind, ok := awsKind(it.InstanceType)
			if !ok {
				continue
			}
			price, err := strconv.ParseFloat(it.SpotPrice, 64)
			if err != nil || price == 0 {
				continue
			}
			spec := specs[it.InstanceType] // zero-value if on-demand lacked it
			out = append(out, feed.Offering{
				Provider:    "aws",
				SKU:         it.InstanceType,
				Kind:        kind,
				Region:      it.AvailabilityZone,
				GPU:         spec.GPU,
				GPUCount:    spec.GPUCount,
				VCPU:        spec.VCPU,
				MemoryMB:    spec.MemoryMB,
				Reliability: feed.Spot,
				USDPerHour:  price,
				Available:   true,
				ObservedAt:  now,
			})
		}
	}
	return out, nil
}

// ec2SpotPage performs one SigV4-signed DescribeSpotPriceHistory call.
func ec2SpotPage(ctx context.Context, client *http.Client, signer *v4.Signer, creds aws.Credentials, region, token string) (*ec2SpotResponse, error) {
	form := url.Values{}
	form.Set("Action", "DescribeSpotPriceHistory")
	form.Set("Version", ec2APIVersion)
	form.Set("StartTime", time.Now().UTC().Format("2006-01-02T15:04:05Z"))
	form.Set("ProductDescription.1", "Linux/UNIX")
	form.Set("Filter.1.Name", "instance-type")
	for i, glob := range awsSpotFamilyGlobs() {
		form.Set(fmt.Sprintf("Filter.1.Value.%d", i+1), glob)
	}
	if token != "" {
		form.Set("NextToken", token)
	}
	body := form.Encode()

	endpoint := "https://ec2." + region + ".amazonaws.com/"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	sum := sha256.Sum256([]byte(body))
	if err := signer.SignHTTP(ctx, creds, req, hex.EncodeToString(sum[:]), "ec2", region, time.Now()); err != nil {
		return nil, fmt.Errorf("sign: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ec2 %s: status %d: %s", region, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out ec2SpotResponse
	if err := xml.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("decode ec2 xml: %w", err)
	}
	return &out, nil
}

// awsSpotFamilyGlobs turns the family allowlist into EC2 instance-type
// filter globs (e.g. "g6.*") so the spot API only returns those.
func awsSpotFamilyGlobs() []string {
	out := make([]string, 0, len(awsFamilies))
	for fam := range awsFamilies {
		out = append(out, fam+".*")
	}
	return out
}

// --- shared helpers ------------------------------------------------------

// awsKind maps an EC2 instance type to a yscale abstract kind via its
// family (the token before the "."). Returns ok=false for families not
// in the allowlist.
func awsKind(instanceType string) (string, bool) {
	fam := instanceType
	if i := strings.IndexByte(instanceType, '.'); i > 0 {
		fam = instanceType[:i]
	}
	k, ok := awsFamilies[fam]
	return k, ok
}

// parseGiB converts an AWS memory string like "16 GiB" to megabytes.
func parseGiB(s string) int {
	f := strings.Fields(s)
	if len(f) == 0 {
		return 0
	}
	v, err := strconv.ParseFloat(strings.ReplaceAll(f[0], ",", ""), 64)
	if err != nil {
		return 0
	}
	return int(v * 1024)
}
