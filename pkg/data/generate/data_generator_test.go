package generate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

func TestDataGenerator_Generate(t *testing.T) {
	g := &DataGenerator{}
	assert.NoError(t, g.Generate(&common.DataGeneratorConfig{
		LogInterval:          time.Second,
		InterleavedNumGroups: 10,
		BaseConfig: common.BaseConfig{
			Scale: 2,
			Use:   "devops",
		},
	}))
}
