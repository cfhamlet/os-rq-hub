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

	hubLifecycle := func(lc fx.Lifecycle, hub *core.Core, r *runner.Runner) {
		runner.ServeFlowLifecycle(lc, hub, r)
	}

	var r *runner.Runner

	app := fx.New(
		fx.Provide(
			runner.New,
			newConfig,
			utils.NewRedisClient,
			core.New,
			ginserv.NewEngine,
			ginserv.NewServer,
			ginserv.NewAPIGroup,
		),
		fx.Invoke(
			config.PrintDebugConfig,
			log.ConfigLogging,
			ginserv.LoadGlobalMiddlewares,
			router.InitAPIRouter,
			hubLifecycle,
			runner.HTTPServerLifecycle,
		),
		fx.Populate(&r),
	)

	r.Run(app)
}
