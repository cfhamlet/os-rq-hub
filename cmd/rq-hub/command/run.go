package command

import (
	"context"

	"github.com/cfhamlet/os-rq-hub/app/router"
	core "github.com/cfhamlet/os-rq-hub/hub"
	defaultConfig "github.com/cfhamlet/os-rq-hub/internal/config"
	"github.com/cfhamlet/os-rq-pod/pkg/command"
	"github.com/cfhamlet/os-rq-pod/pkg/config"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv"
	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/runner"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v7"

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

	newHub := func(lc fx.Lifecycle, conf *viper.Viper, client *redis.Client) (hub *core.Hub, err error) {
		hub, err = core.NewHub(conf, client)
		if err != nil {
			return
		}
		lc.Append(
			fx.Hook{
				OnStart: func(context.Context) error {
					return hub.OnStart()
				},
				OnStop: func(ctx context.Context) error {
					return hub.OnStop()
				},
			})
		return
	}

	app := fx.New(
		fx.Provide(
			loadConfig,
			utils.NewRedisClient,
			newHub,
			newEngine,
			ginserv.NewServer,
			ginserv.NewAPIGroup,
			runner.HTTPServerLifecycle,
		),
		fx.Invoke(
			config.PrintDebugConfig,
			log.ConfigLogging,
			ginserv.LoadGlobalMiddlewares,
			router.InitAPIRouter,
		),
		fx.Populate(&startFail),
	)

	runner.Run(app, startFail)
}
