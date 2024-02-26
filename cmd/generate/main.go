package main

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/timescale/tsbs/pkg/data/usecases/common"

	"github.com/lindb/tsdb-tester/pkg/data/generate"
	"github.com/lindb/tsdb-tester/pkg/util"
)

var (
	config = &common.DataGeneratorConfig{}
)

// Parse args:
func init() {
	config.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := util.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&config.BaseConfig); err != nil {
		panic(fmt.Errorf("unable to decode base config: %s", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
}

func main() {
	dg := &generate.DataGenerator{}
	err := dg.Generate(config)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}
