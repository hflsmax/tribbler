package libstore

import (
    "errors"

    "container/list"
    "github.com/cmu440/tribbler/rpc/librpc"
    "github.com/cmu440/tribbler/rpc/storagerpc"
    "net/rpc"
    "sync"
    "time"
)

type value struct {
    content  string
    contents []string
    lease    storagerpc.Lease
    start    time.Time
    queries  *list.List
    lock     *sync.Mutex
}

type libstore struct {
    mode    LeaseMode
    client  *rpc.Client
    servers []storagerpc.Node
    cache   map[string]*value
    clients map[string]*rpc.Client
    lock    *sync.RWMutex
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).

func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
    ls := new(libstore)
    cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
    rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
    if err != nil {
        return nil, err
    }
    ls.mode = mode
    ls.client = cli
    cache := make(map[string]*value)
    ls.cache = cache
    ls.clients = make(map[string]*rpc.Client)
    ls.clients[masterServerHostPort] = ls.client
    ls.lock = &sync.RWMutex{}
    args := &storagerpc.GetServersArgs{}
    var reply storagerpc.GetServersReply
    count := 0
    err = ls.client.Call("StorageServer.GetServers", args, &reply)
    if reply.Status == storagerpc.OK {
        ls.servers = reply.Servers
    } else {
        for count < 5 {
            count += 1
            time.Sleep(time.Duration(time.Second))
            err = ls.client.Call("StorageServer.GetServers", args, &reply)
            if reply.Status == storagerpc.OK {
                ls.servers = reply.Servers
                break
            }
        }
        if count == 5 {
            return nil, errors.New("server not ready")
        }
    }

    go cleanCache(ls)
    return ls, err
}

func (ls *libstore) Get(key string) (string, error) {
	ls.lock.RLock()
    v, _, err := get(key, 0, ls)
    ls.lock.RUnlock()
    return v, err
}

func (ls *libstore) Put(key, value string) error {
    args := &storagerpc.PutArgs{key, value}
    var reply storagerpc.PutReply
    hostport := findNode(key, ls.servers).HostPort
    cli, ok := ls.clients[hostport]
    if ok == false {
        cli, _ = rpc.DialHTTP("tcp", hostport)
        ls.clients[hostport] = cli
    }
    cli.Call("StorageServer.Put", args, &reply)
    if reply.Status == storagerpc.OK {
        return nil
    } else {
        return errors.New("not put")
    }
}

func (ls *libstore) Delete(key string) error {
    args := &storagerpc.DeleteArgs{key}
    var reply storagerpc.DeleteReply
    hostport := findNode(key, ls.servers).HostPort
    cli, ok := ls.clients[hostport]
    if ok == false {
        cli, _ = rpc.DialHTTP("tcp", hostport)
        ls.clients[hostport] = cli
    }
    cli.Call("StorageServer.Delete", args, &reply)
    if reply.Status == storagerpc.OK {
        return nil
    } else {
        return errors.New("not delete")
    }
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.lock.RLock()
    _, v, err := get(key, 1, ls)
    ls.lock.RUnlock()
    return v, err
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
    args := &storagerpc.PutArgs{key, removeItem}
    var reply storagerpc.PutReply
    hostport := findNode(key, ls.servers).HostPort
    cli, ok := ls.clients[hostport]
    if ok == false {
        cli, _ = rpc.DialHTTP("tcp", hostport)
        ls.clients[hostport] = cli
    }
    cli.Call("StorageServer.RemoveFromList", args, &reply)
    if reply.Status == storagerpc.OK {
        return nil
    } else {
        return errors.New("not RemoveFromList")
    }
}

func (ls *libstore) AppendToList(key, newItem string) error {
    args := &storagerpc.PutArgs{key, newItem}
    var reply storagerpc.PutReply
    hostport := findNode(key, ls.servers).HostPort
    cli, ok := ls.clients[hostport]
    if ok == false {
        cli, _ = rpc.DialHTTP("tcp", hostport)
        ls.clients[hostport] = cli
    }
    cli.Call("StorageServer.AppendToList", args, &reply)
    if reply.Status == storagerpc.OK {
        return nil
    } else {
        return errors.New("not AppendToList")
    }
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
    key := args.Key
    ls.lock.Lock()
    v, ok := ls.cache[key]
    if ok == false {
        reply.Status = storagerpc.KeyNotFound
    } else {
        v.lease.ValidSeconds = 0
        reply.Status = storagerpc.OK
    }
    ls.lock.Unlock()
    return nil
}

func findNode(key string, servers []storagerpc.Node) storagerpc.Node {
    hash := StoreHash(key)
    var min uint32 = 4294967295
    var node storagerpc.Node = servers[0]
    for _, item := range servers {
        if item.NodeID-hash < min {
            node = item
            min = item.NodeID - hash
        }
    }
    return node
}

func get(key string, queryType int, ls *libstore) (string, []string, error) {
    v, ok := ls.cache[key]
    if ok {
    	v.lock.Lock()
    }
    var valid float64
    if ok && v.lease.ValidSeconds != 0 {
        valid = float64(v.lease.ValidSeconds) - time.Now().Sub(v.start).Seconds()
    } else {
        valid = 0
    }
    if ls.mode != Never && ok == true && valid > 0 && v.lease.Granted {
        if queryType == 0 {
        	v.lock.Unlock()
            return v.content, nil, nil
        } else {
        	v.lock.Unlock()
            return "", v.contents, nil
        }
    } else {
        var n time.Time
        if ls.mode != Never {
            n = time.Now()
            if ok && valid == 0 {
                ls.cache[key].content = ""
                ls.cache[key].contents = nil
            } else {
                ls.cache[key] = new(value)
                ls.cache[key].queries = list.New()
                ls.cache[key].lock = &sync.Mutex{}
                ls.cache[key].lock.Lock()
            }
            v = ls.cache[key]
            v.queries.PushFront(n)
        }
        count := 0
        if ls.mode == Normal {
            if v.queries.Len() > storagerpc.QueryCacheThresh {
                v.queries.Remove(v.queries.Back())
            }
            for e := v.queries.Front(); e != nil; e = e.Next() {
                t := e.Value.(time.Time)
                if n.Sub(t).Seconds() < float64(storagerpc.QueryCacheSeconds) {
                    count += 1
                } else {
                    v.queries.Remove(e)
                }
            }
        } else if ls.mode == Never {
            count = 0
        } else {
            count = storagerpc.QueryCacheThresh
        }
        hostport := findNode(key, ls.servers).HostPort
        cli, ok := ls.clients[hostport]
        if ok == false {
            cli, _ = rpc.DialHTTP("tcp", hostport)
            ls.clients[hostport] = cli
        }
        if count >= storagerpc.QueryCacheThresh {
            args := &storagerpc.GetArgs{key, true, hostport}
            if queryType == 0 {
                var reply storagerpc.GetReply
                err := cli.Call("StorageServer.Get", args, &reply)
                if reply.Status == storagerpc.OK {
                    v.content = reply.Value
                    v.lease = reply.Lease
                    v.start = time.Now()
                } else {
                	v.lock.Unlock()
                    return reply.Value, nil, errors.New("not get")
                }
                v.lock.Unlock()
                return reply.Value, nil, err
            } else {
                var reply storagerpc.GetListReply
                err := cli.Call("StorageServer.GetList", args, &reply)
                if reply.Status == storagerpc.OK {
                    v.contents = reply.Value
                    v.lease = reply.Lease
                    v.start = time.Now()
                } else {
                	v.lock.Unlock()
                    return "", reply.Value, errors.New("not get list")
                }
                v.lock.Unlock()
                return "", reply.Value, err
            }
        } else {
        	if ls.mode != Never {
        		v.lock.Unlock()
        	}
            args := &storagerpc.GetArgs{key, false, hostport}
            if queryType == 0 {
                var reply storagerpc.GetReply
                err := cli.Call("StorageServer.Get", args, &reply)
                if reply.Status == storagerpc.OK {
                    return reply.Value, nil, err
                } else {
                    return reply.Value, nil, errors.New("not get")
                }
            } else {
                var reply storagerpc.GetListReply
                err := cli.Call("StorageServer.GetList", args, &reply)
                if reply.Status == storagerpc.OK {
                    return "", reply.Value, err
                } else {
                    return "", reply.Value, errors.New("not get list")
                }
            }
        }
    }
}

func cleanCache(ls *libstore) {
    for {
        time.Sleep(time.Duration(time.Second))
        ls.lock.Lock()
        n := time.Now()
        for k, v := range ls.cache {
            for e := v.queries.Front(); e != nil; e = e.Next() {
                t := e.Value.(time.Time)
                if n.Sub(t).Seconds() >= float64(storagerpc.QueryCacheSeconds) {
                    v.queries.Remove(e)
                }
            }
            if v.queries.Len() == 0 {
                delete(ls.cache, k)
            }
        }
        ls.lock.Unlock()
    }
}