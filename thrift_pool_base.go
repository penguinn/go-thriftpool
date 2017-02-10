package thrift_pool

import (
	"git.apache.org/thrift.git/lib/go/thrift"
	"sync"
	"time"
	"errors"
	"math/rand"
	"reflect"
)

var (
	ErrPoolClosed                  = errors.New("pool has been closed")
	ErrPoolMaxOpenReached          = errors.New("pool max open client limit reached")
	ErrClientMissingTransportField = errors.New("client missing transport field")
	ErrClientNilTransportField     = errors.New("client transport field is nil")
	errNoPooledClient              = errors.New("No pooled client")
	ErrEmptyInst				   = errors.New("Cant use Empty Inst")
)

type Client interface{}

type ClientFactory func(openedSocket thrift.TTransport) Client

type ThriftConnectionPool struct{
	poolMutex 			sync.Mutex
	mapClientChannels 	map[string]chan Client
	maxInst				uint32
	mapActiveInst		map[string]uint32
	connectTimeout		time.Duration
	clientFactory 		ClientFactory
}

func NewChannelClientPool(maxInst uint32, connectTimeout time.Duration, clientFactory ClientFactory) *ThriftConnectionPool {
	pool := &ThriftConnectionPool{
		mapClientChannels:		make(map[string]chan Client, maxInst),
		maxInst:        		maxInst,
		mapActiveInst:        	make(map[string]uint32),
		connectTimeout: 		connectTimeout,
		clientFactory:  		clientFactory,
	}
	return pool
}

func (p *ThriftConnectionPool) GetOne(hosts []string) (clientManager *ClientManager, err error) {
	//先从池子中取
	rawCli, host, err := p.getFromPool(hosts)

	//如果取不到就新建
	if rawCli == nil {
		rawCli, host, err = p.newInst(hosts)
		if err != nil {
			return
		}
	}
	clientManager = &ClientManager{
		Client:	rawCli,
		host:   host,
		pool:   p,
	}

	return
}

func (p *ThriftConnectionPool) ReleaseOne(clientManager *ClientManager, host string) (err error) {
	if p.maxInst == 0 {
		return ErrEmptyInst
	}
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	if clientManager.unusable{
		p.closeClient(clientManager.Client, host)
		return
	}

	clientChannels, ok := p.mapClientChannels[host]
	if ok {
		select {
		case clientChannels <- clientManager.Client:
			clientManager.Client = nil
		}
	} else{
		clientChannels = make(chan Client, p.maxInst)
		p.mapClientChannels[host] = clientChannels
		select {
		case clientChannels <- clientManager.Client:
			clientManager.Client = nil
		}
	}

	return
}

func (p *ThriftConnectionPool) Size() (mapHostInst map[string]int) {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	mapHostInst = make(map[string]int)
	for host, clientChannels := range p.mapClientChannels{
		mapHostInst[host] = len(clientChannels)
	}
	return
}

func (p *ThriftConnectionPool) Close() (err error) {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()

	if p.mapClientChannels == nil {
		return
	}
	for host, clientChannels := range p.mapClientChannels {
		//关闭长连接
		//todo release
		for client := range clientChannels {
			err = p.closeClient(client, host)
		}
		//关闭channel
		close(clientChannels)
	}
	p.mapClientChannels = nil
	return
}

func (p *ThriftConnectionPool) getFromPool(hosts []string) (rawCli Client, host string, err error) {
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()
	if p.mapClientChannels == nil {
		return nil, "", ErrPoolClosed
	}

	mapIndex := 0
	for rawCli == nil && mapIndex < len(hosts) {
		select {
		//这里先假设channel不会被关闭，所以没用ok来判断
		case rawCli = <-p.mapClientChannels[hosts[mapIndex]]:
			host = hosts[mapIndex]
		default:
			mapIndex++
		}
	}
	return
}

func (p *ThriftConnectionPool) newInst(hosts []string) (rawCli Client, host string, err error) {
	if p.maxInst == 0 {
		return nil, "", ErrPoolMaxOpenReached
	}
	isIdleInst := false
	hostIndex := 0
	p.poolMutex.Lock()
	defer p.poolMutex.Unlock()

	hostLen := len(hosts)
	offset := rand.Int()%hostLen
	hostIndex = offset

	for hostIndex < hostLen+offset {
		if p.mapActiveInst[hosts[hostIndex%hostLen]] >= p.maxInst {
			hostIndex++
		} else {
			host = hosts[hostIndex%hostLen]
			isIdleInst = true
			break
		}
	}

	if isIdleInst == false{
		return nil, "", ErrPoolMaxOpenReached
	}

	var transport *thrift.TSocket
	if transport, err = thrift.NewTSocket(host); err != nil {
		return
	}
	if err = transport.Open(); err != nil {
		return
	}
	p.mapActiveInst[host]++

	return p.clientFactory(transport), host, nil
}

func (p *ThriftConnectionPool) closeClient(rawCli Client, host string) (err error) {
	if rawCli == nil {
		return nil
	}
	if p.maxInst != 0 {
		p.mapActiveInst[host]--
	}
	if v := reflect.ValueOf(rawCli).Elem().FieldByName("Transport"); !v.IsValid() {
		return ErrClientMissingTransportField
	} else if v.IsNil() {
		return ErrClientNilTransportField
	} else {
		if transport, ok := v.Interface().(thrift.TTransport); !ok {
			panic(v)
		} else {
			return transport.Close()
		}
	}
}

