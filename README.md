go-thriftpool
========

这是一个简单的go语言thrift客户端连接池，利用函数NewChannelClientPool注册一个池子，利用函数Getone传进来的hosts(一个host的数组)动态的返回一个可用thrift连接，当你后一次传入的hosts数组不同时，对前面的连接没有影响