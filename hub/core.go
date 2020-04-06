package hub

import (
	"sync"

	"github.com/cfhamlet/os-rq-pod/pkg/log"
	"github.com/cfhamlet/os-rq-pod/pkg/serv"
	"github.com/cfhamlet/os-rq-pod/pkg/sth"
	"github.com/go-redis/redis/v7"
	"github.com/spf13/viper"
)

// Core TODO
type Core struct {
	*serv.RedisServ
	UpstreamMgr   *UpstreamManager
	DownstreamMgr *DownstreamManager
	waitStop      *sync.WaitGroup
}

// New TODO
func New(conf *viper.Viper, client *redis.Client) (core *Core, err error) {

	core = &Core{
		serv.NewRedisServ(conf, client),
		nil,
		nil,
		&sync.WaitGroup{},
	}

	return
}

// OnStart TODO
func (core *Core) OnStart() (err error) {
	err = core.SetStatus(serv.Preparing, true)
	if err == nil {
		for name, ext := range map[string]serv.IExtension{
			"upstream":   NewUpstreamManager(core),
			"downstream": NewDownstreamManager(core),
		} {
			core.AddExtension(name, ext)
		}
		err = core.Setup()
	}

	if err == nil {
		err = core.SetStatus(serv.Working, true)
	}

	switch err.(type) {
	case *serv.StatusConflictError:
		if serv.StopStatus(core.Status(true)) {
			log.Logger.Warning("stop when starting")
			return nil
		}
	}

	return
}

// OnStop TODO
func (core *Core) OnStop() error {
	_, err := core.DoWithLock(
		func() (interface{}, error) {
			err := core.SetStatus(serv.Stopping, false)
			if err == nil {
				err = core.Cleanup()
				if err == nil {
					core.waitStop.Wait()
					err = core.SetStatus(serv.Stopped, false)
				}
			}
			return nil, err

		}, false)

	return err
}

// Info TODO
func (core *Core) Info() (sth.Result, error) {
	result, err := core.DoWithLock(
		func() (interface{}, error) {
			result, err := core.MetaInfo()
			result["upstreams_info"] = core.UpstreamMgr.Info()
			return result, err
		}, true)
	return result.(sth.Result), err
}
