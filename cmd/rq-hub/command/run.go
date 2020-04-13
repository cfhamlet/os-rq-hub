package command

import (
	"github.com/cfhamlet/os-rq-hub/app/controllers"
	"github.com/cfhamlet/os-rq-hub/app/routers"
	"github.com/cfhamlet/os-rq-hub/hub/global"
	"github.com/cfhamlet/os-rq-hub/hub/upstream"
	podctrl "github.com/cfhamlet/os-rq-pod/app/controllers"
	podroute "github.com/cfhamlet/os-rq-pod/app/routers"
	"github.com/cfhamlet/os-rq-pod/pkg/command"
	"github.com/cfhamlet/os-rq-pod/pkg/config"
	"github.com/cfhamlet/os-rq-pod/pkg/ginserv"
	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/runner"
	"github.com/cfhamlet/os-rq-pod/pkg/serv"
	"github.com/cfhamlet/os-rq-pod/pkg/utils"
	"github.com/cfhamlet/os-rq-pod/pod/redisconfig"

	"github.com/go-redis/redis/v7"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func init() {
	Root.AddCommand(command.NewRunCommand("rq-hub", run))
}

func run(conf *viper.Viper) {
	newConfig := func() (*viper.Viper, error) {
		err := config.LoadConfig(conf, global.EnvPrefix, global.DefaultConfig)
		return conf, err
	}

	newRdsConfig := func(serv *serv.Serv, client *redis.Client) *redisconfig.RedisConfig {
		return redisconfig.New(serv, client, []string{})
	}

	hubGo := func(lc fx.Lifecycle, serv *serv.Serv, r *runner.Runner) {
		runner.ServWait(lc, serv, r)
	}

	upmgrGo := func(lc fx.Lifecycle, upMgr *upstream.Manager, r *runner.Runner) {
		runner.ServGo(lc, upMgr, r)
	}

	var r *runner.Runner

	app := fx.New(
		fx.Provide(
			runner.New,
			newConfig,
			utils.NewRedisClient,
			newRdsConfig,
			serv.New,
			upstream.NewManager,
			ginserv.NewEngine,
			ginserv.NewServer,
			ginserv.NewAPIGroup,
			controllers.NewQueuesController,
			controllers.NewRequestController,
			controllers.NewUpstreamController,
			podctrl.NewConfigController,
			podctrl.NewServController,
			podctrl.NewRedisController,
		),
		fx.Invoke(
			config.PrintDebugConfig,
			log.ConfigLogging,
			ginserv.LoadGlobalMiddlewares,
			upmgrGo,
			hubGo,
			routers.RouteQueuesCtrl,
			routers.RouteRequestCtrl,
			routers.RouteUpstreamCtrl,
			routers.RouteUpstreamsCtrl,
			podroute.RouteRedisCtrl,
			podroute.RouteConfigCtrl,
			podroute.RouteServCtrl,
			runner.HTTPServerLifecycle,
		),
		fx.Populate(&r),
	)

	r.Run(app)
}
