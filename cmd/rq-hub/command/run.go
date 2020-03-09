package command

import (
	"github.com/cfhamlet/os-rq-hub/core"
	defaultConfig "github.com/cfhamlet/os-rq-hub/internal/config"
	"github.com/cfhamlet/os-rq-pod/pkg/command"
	"github.com/cfhamlet/os-rq-pod/pkg/config"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv"
	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/runner"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/gin-gonic/gin"

	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func init() {
	Root.AddCommand(command.NewRunCommand("rq-hub", run))
}

var startFail chan error

func run(conf *viper.Viper) {
	loadConfig := func() (*viper.Viper, error) {
		err := config.LoadConfig(conf, defaultConfig.EnvPrefix, defaultConfig.DefaultConfig)
		return conf, err
	}

	newEngine := func(*core.Hub) *gin.Engine {
		return ginserv.NewEngine(conf)
	}

	app := fx.New(
		fx.Provide(
			loadConfig,
			utils.NewRedisClient,
			core.NewHub,
			newEngine,
			ginserv.NewServer,
			ginserv.NewAPIGroup,
			runner.HTTPServerLifecycle,
		),
		fx.Invoke(
			config.PrintDebugConfig,
			log.ConfigLogging,
			ginserv.LoadGlobalMiddlewares,
		),
		fx.Populate(&startFail),
	)

	runner.Run(app, startFail)
}
