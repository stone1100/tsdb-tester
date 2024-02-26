package generate

import (
	"bytes"
	"compress/gzip"
	_ "embed"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"github.com/go-resty/resty/v2"
	"github.com/lindb/common/proto/gen/v1/flatMetricsV1"
	"github.com/lindb/common/series"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

//go:embed iot.csv
var IOTFieldMappingData string

//go:embed devops.csv
var DevOpsFieldMappingData string

const (
	defaultTimeStart = "2023-12-13T00:00:00Z"
	defaultTimeEnd   = "2023-12-16T00:00:00Z"
)

var (
	IOTFieldMapping    map[string]float64
	DevOpsFieldMapping map[string]float64
)

func init() {
	IOTFieldMapping = mapping(IOTFieldMappingData)
	DevOpsFieldMapping = mapping(DevOpsFieldMappingData)
}

func mapping(data string) map[string]float64 {
	result := make(map[string]float64)
	rows := strings.Split(data, "\n")
	for _, row := range rows {
		columns := strings.Split(row, ",")
		if len(columns) == 2 {
			val, err := strconv.ParseFloat(columns[1], 64)
			if err != nil {
				panic(err)
			}
			result[columns[0]] = val
		}
	}
	return result
}

// Error messages when using a DataGenerator
const (
	ErrNoConfig          = "no GeneratorConfig provided"
	ErrInvalidDataConfig = "invalid config: DataGenerator needs a DataGeneratorConfig"
)

type DataGenerator struct {
	config       *common.DataGeneratorConfig
	builder      *series.RowBuilder
	fieldMapping map[string]float64

	cli         *resty.Client
	buf         *bytes.Buffer
	gzipBuf     *bytes.Buffer
	gzipWriter  *gzip.Writer
	batchedSize int
}

func (g *DataGenerator) init(config common.GeneratorConfig) error {
	if config == nil {
		return fmt.Errorf(ErrNoConfig)
	}
	switch config.(type) {
	case *common.DataGeneratorConfig:
	default:
		return fmt.Errorf(ErrInvalidDataConfig)
	}
	g.config = config.(*common.DataGeneratorConfig)
	if g.config.Scale == 0 {
		return fmt.Errorf(common.ErrScaleIsZero)
	}

	// set field mapping based on usecase
	switch g.config.Use {
	case "devops":
		g.fieldMapping = DevOpsFieldMapping
	case "iot":
		g.fieldMapping = IOTFieldMapping
	}

	g.config.TimeStart = defaultTimeStart
	g.config.TimeEnd = defaultTimeEnd
	fmt.Println(g.config.MaxMetricCountPerHost)
	g.builder = series.CreateRowBuilder()
	g.buf = &bytes.Buffer{}
	g.gzipBuf = &bytes.Buffer{}
	g.gzipWriter = gzip.NewWriter(g.gzipBuf)
	g.cli = resty.New()
	g.cli.SetBaseURL("http://localhost:9000/api/v1/write?db=_internal")
	return nil
}

func (g *DataGenerator) Generate(config common.GeneratorConfig) error {
	err := g.init(config)
	if err != nil {
		return err
	}

	rand.Seed(g.config.Seed)

	scfg, err := usecases.GetSimulatorConfig(g.config)
	if err != nil {
		return err
	}
	// c := scfg.(*devops.DevopsSimulatorConfig)
	fmt.Printf("%t\n", scfg)

	sim := scfg.NewSimulator(g.config.LogInterval, g.config.Limit)
	fmt.Printf("%t\n", sim)

	return g.runSimulator(sim, g.config)
}

func (g *DataGenerator) runSimulator(sim common.Simulator, dgc *common.DataGeneratorConfig) error {
	currGroupID := uint(0)
	point := data.NewPoint()
	for !sim.Finished() {
		write := sim.Next(point)
		if !write {
			point.Reset()
			continue
		}

		// in the default case this is always true
		if currGroupID == dgc.InterleavedGroupID {
			g.writePoint(point)
		}
		point.Reset()

		currGroupID = (currGroupID + 1) % dgc.InterleavedNumGroups
	}

	// flush pending point
	g.flushBuffer()
	return nil
}

func (g *DataGenerator) writePoint(p *data.Point) {
	defer g.builder.Reset()

	g.builder.AddMetricName(p.MeasurementName())
	g.builder.AddTimestamp(p.Timestamp().UnixMilli())

	// write tags
	tagKeys := p.TagKeys()
	tagValues := p.TagValues()
	for i := 0; i > len(tagKeys); i++ {
		switch v := tagValues[i].(type) {
		case string:
			fmt.Println(string(tagKeys[i]))
			fmt.Println(string(p.MeasurementName()))
			fmt.Printf("k=%s,v=%s\n", tagKeys[i], string(v))
			if err0 := g.builder.AddTag(tagKeys[i], []byte(v)); err0 != nil {
				panic(err0)
			}
		case nil:
			continue
		default:
			panic("non-string tags not implemented for lindb")
		}
	}
	// write fields
	fieldKeys := p.FieldKeys()
	fieldCount := 0
	for i := 1; i < len(fieldKeys); i++ {
		fName := fieldKeys[i]
		// generate field value
		val := g.fieldMapping[string(fName)]
		g.builder.AddSimpleField(fName, flatMetricsV1.SimpleFieldTypeLast, val)
		fieldCount++
	}
	// build data point
	data, err := g.builder.Build()
	if err != nil {
		panic(err)
	}

	_, err = g.buf.Write(data)
	if err != nil {
		panic(err)
	}
	g.batchedSize++

	if g.batchedSize > 100 {
		g.flushBuffer()
	}
}

func (g *DataGenerator) flushBuffer() {
	if g.batchedSize == 0 {
		return
	}
	data := g.buf.Bytes()
	g.buf.Reset() // reset batch buf
	g.batchedSize = 0

	g.gzipBuf.Reset()
	g.gzipWriter.Reset(g.gzipBuf)
	if _, err := g.gzipWriter.Write(data); err != nil {
		panic(err)
	}
	if err := g.gzipWriter.Close(); err != nil {
		panic(err)
	}

	_, err := g.cli.R().
		SetBody(g.gzipBuf.Bytes()).
		SetHeader("Accept", "application/json").
		SetHeader("Content-Type", "application/flatbuffer").
		SetHeader("Content-Encoding", "gzip").
		Put("")
	if err != nil {
		panic(err)
	}
}
