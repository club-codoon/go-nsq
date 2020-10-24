package nsq

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Driver struct {
	addrs           []string
	consumerTopic   string
	consumerChannel string
	consumerHandler Handler
	concurrency     int
	consumers       []*Consumer

	producers     []*Producer
	producerIndex uint32
	mtx           sync.RWMutex

	logger   logger
	logLvl   LogLevel
	logGuard sync.RWMutex

	behaviorDelegate interface{}

	consumerConfig *Config
	producerConfig *Config

	consumerEnable bool
	producerEnable bool

	rngMtx sync.Mutex
	rng    *rand.Rand

	lookupdRecheckChan chan int
	lookupdHTTPAddrs   []string
	lookupdQueryIndex  int

	wg            sync.WaitGroup
	stopFlag      int32
	connectedFlag int32
	stopHandler   sync.Once
	exitHandler   sync.Once

	exitChan chan int
}

func NewDriver(consumerConfig, producerConfig *Config, consumerTopic, consumerChannel string, consumerHandler Handler, concurrency int) *Driver {
	return &Driver{
		consumerTopic:   consumerTopic,
		consumerChannel: consumerChannel,
		consumerConfig:  consumerConfig,
		producerConfig:  producerConfig,
		consumerHandler: consumerHandler,
		concurrency:     concurrency,

		logger:             log.New(os.Stderr, "", log.Flags()),
		logLvl:             LogLevelInfo,
		rng:                rand.New(rand.NewSource(time.Now().UnixNano())),
		lookupdRecheckChan: make(chan int, 1),
		exitChan:           make(chan int),

		consumerEnable: true,
		producerEnable: true,
	}
}

func NewProducerDriver(config *Config) *Driver {
	return &Driver{
		consumerConfig: NewConfig(),
		producerConfig: config,

		logger:             log.New(os.Stderr, "", log.Flags()),
		logLvl:             LogLevelInfo,
		rng:                rand.New(rand.NewSource(time.Now().UnixNano())),
		lookupdRecheckChan: make(chan int, 1),
		exitChan:           make(chan int),

		consumerEnable: false,
		producerEnable: true,
	}
}

func NewConsumerDriver(consumerConfig *Config, consumerTopic, consumerChannel string, consumerHandler Handler, concurrency int) *Driver {
	return &Driver{
		consumerTopic:   consumerTopic,
		consumerChannel: consumerChannel,
		consumerConfig:  consumerConfig,
		consumerHandler: consumerHandler,
		concurrency:     concurrency,

		logger:             log.New(os.Stderr, "", log.Flags()),
		logLvl:             LogLevelInfo,
		rng:                rand.New(rand.NewSource(time.Now().UnixNano())),
		lookupdRecheckChan: make(chan int, 1),
		exitChan:           make(chan int),

		consumerEnable: true,
		producerEnable: false,
	}
}

func (d *Driver) Consumers() []*Consumer {
	return d.consumers
}

func (d *Driver) Producers() []*Producer {
	return d.producers
}

func (d *Driver) Producer() *Producer {
	d.mtx.RLock()
	defer d.mtx.RUnlock()
	if len(d.producers) == 0 {
		return nil
	}
	index := atomic.AddUint32(&d.producerIndex, 1) % uint32(len(d.producers))
	return d.producers[index]
}

func (d *Driver) newConsumer() (*Consumer, error) {
	consumer, err := NewConsumer(d.consumerTopic, d.consumerChannel, d.consumerConfig)
	if err != nil {
		return nil, err
	}
	consumer.AddConcurrentHandlers(d.consumerHandler, d.concurrency)
	return consumer, nil
}

// SetLogger assigns the logger to use as well as a level
//
// The logger parameter is an interface that requires the following
// method to be implemented (such as the the stdlib log.Logger):
//
//    Output(calldepth int, s string)
//
func (d *Driver) SetLogger(l logger, lvl LogLevel) {
	d.logGuard.Lock()
	defer d.logGuard.Unlock()

	d.logger = l
	d.logLvl = lvl
}

func (d *Driver) getLogger() (logger, LogLevel) {
	d.logGuard.RLock()
	defer d.logGuard.RUnlock()

	return d.logger, d.logLvl
}

// SetBehaviorDelegate takes a type implementing one or more
// of the following interfaces that modify the behavior
// of the `Consumer`:
//
//    DiscoveryFilter
//
func (d *Driver) SetBehaviorDelegate(cb interface{}) {
	matched := false

	if _, ok := cb.(DiscoveryFilter); ok {
		matched = true
	}

	if !matched {
		panic("behavior delegate does not have any recognized methods")
	}

	d.behaviorDelegate = cb
}

func (d *Driver) Publish(topic string, body []byte) (err error) {
	var lookup, success bool
	for i := uint16(0); i < d.producerConfig.MaxAttempts; i++ {
		producer := d.Producer()
		if producer == nil {
			lookup = true
			err = fmt.Errorf("nil producer")
			continue
		}
		err = producer.Publish(topic, body)
		if err != nil {
			lookup = true
			continue
		}
		success = true
		break
	}
	if lookup {
		d.lookupdRecheckChan <- 1
	}
	if success {
		return nil
	}
	return fmt.Errorf("nsq publish failed after %v attempts - %v", d.producerConfig.MaxAttempts, err)
}

func (d *Driver) Stop() {
	if !atomic.CompareAndSwapInt32(&d.stopFlag, 0, 1) {
		return
	}

	d.exit()

	for _, c := range d.consumers {
		c.Stop()
	}
}

func (r *Driver) exit() {
	r.exitHandler.Do(func() {
		close(r.exitChan)
		r.wg.Wait()
	})
}

// ConnectToNSQLookupd adds an nsqlookupd address to the list for this Driver instance.
//
// If it is the first to be added, it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (d *Driver) ConnectToNSQLookupd(addr string) error {
	if atomic.LoadInt32(&d.stopFlag) == 1 {
		return errors.New("driver stopped")
	}

	if err := validatedLookupAddr(addr); err != nil {
		return err
	}

	atomic.StoreInt32(&d.connectedFlag, 1)

	d.mtx.Lock()
	for _, x := range d.lookupdHTTPAddrs {
		if x == addr {
			d.mtx.Unlock()
			return nil
		}
	}
	d.lookupdHTTPAddrs = append(d.lookupdHTTPAddrs, addr)
	numLookupd := len(d.lookupdHTTPAddrs)
	d.mtx.Unlock()

	// if this is the first one, kick off the go loop
	if numLookupd == 1 {
		d.queryLookupd()
		d.wg.Add(1)
		go d.lookupdLoop()
	}

	return nil
}

// ConnectToNSQLookupds adds multiple nsqlookupd address to the list for this Driver instance.
//
// If adding the first address it initiates an HTTP request to discover nsqd
// producers for the configured topic.
//
// A goroutine is spawned to handle continual polling.
func (d *Driver) ConnectToNSQLookupds(addresses []string) error {
	for _, addr := range addresses {
		err := d.ConnectToNSQLookupd(addr)
		if err != nil {
			return err
		}
	}
	return nil
}

// poll all known lookup servers every LookupdPollInterval
func (d *Driver) lookupdLoop() {
	// add some jitter so that multiple consumers discovering the same topic,
	// when restarted at the same time, dont all connect at once.
	d.rngMtx.Lock()
	jitter := time.Duration(int64(d.rng.Float64() *
		d.consumerConfig.LookupdPollJitter * float64(d.consumerConfig.LookupdPollInterval)))
	d.rngMtx.Unlock()
	var ticker *time.Ticker

	select {
	case <-time.After(jitter):
	case <-d.exitChan:
		goto exit
	}

	ticker = time.NewTicker(d.consumerConfig.LookupdPollInterval)

	for {
		select {
		case <-ticker.C:
			d.queryLookupd()
		case <-d.lookupdRecheckChan:
			d.queryLookupd()
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	if ticker != nil {
		ticker.Stop()
	}
	d.log(LogLevelInfo, "exiting lookupdLoop")
	d.wg.Done()
}

// return the next lookupd endpoint to query
// keeping track of which one was last used
func (d *Driver) nextLookupdEndpoint() (string, string) {
	d.mtx.RLock()
	if d.lookupdQueryIndex >= len(d.lookupdHTTPAddrs) {
		d.lookupdQueryIndex = 0
	}
	addr := d.lookupdHTTPAddrs[d.lookupdQueryIndex]
	num := len(d.lookupdHTTPAddrs)
	d.mtx.RUnlock()
	d.lookupdQueryIndex = (d.lookupdQueryIndex + 1) % num

	urlString := addr
	if !strings.Contains(urlString, "://") {
		urlString = "http://" + addr
	}

	u, err := url.Parse(urlString)
	if err != nil {
		panic(err)
	}
	if u.Path == "/" || u.Path == "" {
		u.Path = "/nodes"
	}

	v, err := url.ParseQuery(u.RawQuery)
	u.RawQuery = v.Encode()
	return addr, u.String()
}

// make an HTTP req to one of the configured nsqlookupd instances to discover
// which nsqd's provide the topic we are consuming.
//
// initiate a connection to any new producers that are identified.
func (d *Driver) queryLookupd() {
	_, endpoint := d.nextLookupdEndpoint()
	// d.log(LogLevelInfo, "querying nsqlookupd %s", endpoint)

	var data lookupResp
	err := apiRequestNegotiateV1("GET", endpoint, nil, &data)
	if err != nil {
		d.log(LogLevelError, "error querying nsqlookupd (%s) - %s", endpoint, err)
		return
	}

	var nsqdAddrs []string
	for _, producer := range data.Producers {
		broadcastAddress := producer.BroadcastAddress
		port := producer.TCPPort
		joined := net.JoinHostPort(broadcastAddress, strconv.Itoa(port))
		nsqdAddrs = append(nsqdAddrs, joined)
	}
	// apply filter
	if discoveryFilter, ok := d.behaviorDelegate.(DiscoveryFilter); ok {
		nsqdAddrs = discoveryFilter.Filter(nsqdAddrs)
	}

	d.updateConsumers(nsqdAddrs)
	d.updateProducers(nsqdAddrs)
}

func (d *Driver) updateConsumers(addrs []string) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	if !d.consumerEnable {
		return
	}

	for i := 0; i < len(d.addrs); i++ {
		found := false
		for _, addr := range addrs {
			if d.addrs[i] == addr {
				found = true
				break
			}
		}
		if !found {
			go d.consumers[i].Stop()
			d.addrs = append(d.addrs[:i], d.addrs[i+1:]...)
			i--
		}
	}

NSQDADDRS:
	for _, addr := range addrs {
		for _, address := range d.addrs {
			if address == addr {
				// AlreadyConnected
				continue NSQDADDRS
			}
		}

		consumer, err := d.newConsumer()
		if err != nil {
			d.log(LogLevelError, "(%s) error new nsq consumer - %s", addr, err)
			continue
		}
		err = consumer.ConnectToNSQD(addr)
		if err != nil && err != ErrAlreadyConnected {
			go consumer.Stop()
			d.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
			continue
		}
		d.consumers = append(d.consumers, consumer)
		d.addrs = append(d.addrs, addr)
	}
}

func (d *Driver) updateProducers(addrs []string) {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	if !d.producerEnable {
		return
	}

	for i := 0; i < len(d.producers); i++ {
		found := false
		for _, addr := range addrs {
			if d.producers[i].String() == addr {
				found = true
				break
			}
		}
		if !found {
			go d.producers[i].Stop()
			d.producers = append(d.producers[:i], d.producers[i+1:]...)
			i--
		}
	}

NSQDADDRS:
	for _, addr := range addrs {
		for _, producer := range d.producers {
			if producer.String() == addr {
				// AlreadyConnected
				continue NSQDADDRS
			}
		}

		producer, err := NewProducer(addr, d.producerConfig)
		if err != nil {
			d.log(LogLevelError, "(%s) error connecting to nsqd - %s", addr, err)
			continue
		}
		err = producer.Ping()
		if err != nil {
			go producer.Stop()
			d.log(LogLevelError, "(%s) error ping nsqd - %s", addr, err)
			continue
		}

		d.producers = append(d.producers, producer)
	}
}

func (d *Driver) log(lvl LogLevel, line string, args ...interface{}) {
	logger, logLvl := d.getLogger()

	if logger == nil {
		return
	}

	if logLvl > lvl {
		return
	}

	logger.Output(2, fmt.Sprintf("%-4s %s",
		lvl, fmt.Sprintf(line, args...)))
}
