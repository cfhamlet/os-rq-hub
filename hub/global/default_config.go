package global

// EnvPrefix for env
const EnvPrefix = "RQ_HUB"

// DefaultConfig TODO
var DefaultConfig = map[string]interface{}{
	"debug":                             false,
	"log.level":                         "info",
	"http.addr":                         ":6788",
	"http.log.enable":                   true,
	"http.api.prefix":                   "",
	"redis.addr":                        "localhost:6379",
	"redis.password":                    "",
	"redis.db":                          0,
	"limit.redis.memory":                1 << 30,
	"limit.queue.num":                   10000,
	"upstream.http.maxidleconnsperhost": 200,
	"upstream.http.maxidleconns":        10000,
	"upstream.http.idleconntimeout":     90,
}
