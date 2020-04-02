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
	if err != nil {
		return
	}

	err = core.initUpstreamMgr()
	if err == nil {
		err = core.initDownstreamMgr()
		if err == nil {
			err = core.SetStatus(serv.Working, true)
		}
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
			if err != nil {
				return nil, err
			}
			if core.DownstreamMgr != nil {
				core.DownstreamMgr.Stop()
			}
			if core.UpstreamMgr != nil {
				core.UpstreamMgr.Stop()
			}
			core.waitStop.Wait()
			err = core.SetStatus(serv.Stopped, false)
			return nil, err

		}, false)

	return err
}

func (core *Core) initUpstreamMgr() error {
	upstreamMgr := NewUpstreamManager(core)
	err := upstreamMgr.Load()
	if err == nil {
		_, err = core.DoWithLock(
			func() (interface{}, error) {
				err = core.SetStatus(serv.Preparing, false)
				if err == nil {
					core.UpstreamMgr = upstreamMgr
					err = core.UpstreamMgr.Start()
				}
				return nil, err
			}, false)
	}

	return err
}

func (core *Core) initDownstreamMgr() error {
	_, err := core.DoWithLock(
		func() (interface{}, error) {
			err := core.SetStatus(serv.Preparing, false)
			if err == nil {
				core.DownstreamMgr = NewDownstreamManager(core)
				err = core.DownstreamMgr.Start()
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
			result["upstream"] = core.UpstreamMgr.Info()
			return result, err
		}, true)
	return result.(sth.Result), err
}
