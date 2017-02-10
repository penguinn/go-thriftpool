package thrift_pool

type ClientManager struct {
	Client
	pool 	*ThriftConnectionPool
	host 	string
	unusable bool
}
func (p *ClientManager) Close() error {
	return p.pool.ReleaseOne(p, p.host)
}

func (p *ClientManager) RawClient() Client {
	return p.Client
}

// MarkUnusable() marks the connection not usable any more, to let the pool close it instead of returning it to pool.
func (p *ClientManager) MarkUnusable() {
	p.unusable = true
}

func (p *ClientManager) GetHost() string {
	return p.host
}