package command

import (
	"github.com/cfhamlet/os-rq-hub/app/router"
	core "github.com/cfhamlet/os-rq-hub/hub"
	defaultConfig "github.com/cfhamlet/os-rq-hub/internal/config"
	"github.com/cfhamlet/os-rq-pod/pkg/command"
	"github.com/cfhamlet/os-rq-pod/pkg/config"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv"
	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/runner"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"

	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func init() {
	Root.AddCommand(command.NewRunCommand("rq-hub", run))
}

func run(conf *viper.Viper) {
	newConfig := func() (*viper.Viper, error) {
		err := config.LoadConfig(conf, defaultConfig.EnvPrefix, defaultConfig.DefaultConfig)
		return conf, err
	}

	hubLifecycle := func(lc fx.Lifecycle, hub *core.Hub) runner.Ready {
		return runner.ServeFlowLifecycle(lc, hub)
	}

	var failWait runner.FailWait
	app := fx.New(
		fx.Provide(
			newConfig,
			utils.NewRedisClient,
			core.NewHub,
			ginserv.NewEngine,
			ginserv.NewServer,
			ginserv.NewAPIGroup,
			hubLifecycle,
			runner.HTTPServerLifecycle,
		),
		fx.Invoke(
			config.PrintDebugConfig,
			log.ConfigLogging,
			ginserv.LoadGlobalMiddlewares,
			router.InitAPIRouter,
		),
		fx.Populate(&failWait),
	)

	runner.Run(app, failWait)
}
