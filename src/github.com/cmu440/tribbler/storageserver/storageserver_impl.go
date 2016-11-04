package storageserver

import (
	"sync"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"time"
	"strconv"
	"net/http"
	"net"
	// "fmt"
)

type storageServer struct {
	storage map[string]interface{}
	storageMutex *sync.Mutex
	servers []storagerpc.Node
	serversMutex *sync.Mutex
	numNodes int
	allRegisterNotify chan bool
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {
	ss := new(storageServer)
	ss.storage = make(map[string]interface{})
	ss.storageMutex = &sync.Mutex{}
	ss.serversMutex = &sync.Mutex{}

	if masterServerHostPort != "" {
		// This is a slave
		cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}
		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node {
				HostPort: "127.0.0.1:" + strconv.Itoa(port),
				NodeID: nodeID}}
		var reply storagerpc.RegisterReply
		for {
			err = cli.Call("StorageSrver.RegisterServer", args, &reply)
			if reply.Status == storagerpc.OK {
				ss.servers = reply.Servers
				return ss, nil
			}
			time.Sleep(time.Second)
		}
	} else {
		// This is a master
		ss.servers = []storagerpc.Node{storagerpc.Node{
			HostPort: "127.0.0.1:" + strconv.Itoa(port),
			NodeID: nodeID}}
		ss.numNodes = numNodes
		ss.allRegisterNotify = make(chan bool)


		listener, err := net.Listen("tcp", "127.0.0.1:" + strconv.Itoa(port))
    if err != nil {
        return nil, err
    }
    err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
    if err != nil {
        return nil, err
    }
    rpc.HandleHTTP()
    go http.Serve(listener, nil)


		<- ss.allRegisterNotify

		return ss, nil
	}
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.serversMutex.Lock()
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		dulplicate := false
		for _, server := range ss.servers {
			if server.NodeID == args.ServerInfo.NodeID {
				dulplicate = true
				break
			}
		}
		reply.Status = storagerpc.NotReady
		if !dulplicate {
			ss.servers = append(ss.servers, args.ServerInfo)
			if len(ss.servers) == ss.numNodes {
				reply.Status = storagerpc.OK
				reply.Servers = ss.servers
				ss.allRegisterNotify <- true
			}
		}
	}
	ss.serversMutex.Unlock()
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.serversMutex.Lock()
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
	}
	ss.serversMutex.Unlock()
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	ss.storageMutex.Lock()
	if val, ok := ss.storage[args.Key]; ok {
		reply.Status = storagerpc.OK
		reply.Value = val.(string)
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ss.storageMutex.Unlock()
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	ss.storageMutex.Lock()
	if _, ok := ss.storage[args.Key]; ok {
		delete(ss.storage, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ss.storageMutex.Unlock()
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	ss.storageMutex.Lock()
	if val, ok := ss.storage[args.Key]; ok {
		ss.storageMutex.Unlock()
		reply.Status = storagerpc.OK
		reply.Value = val.([]string)
		return nil
	} else {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.storageMutex.Lock()
	ss.storage[args.Key] = args.Value
	ss.storageMutex.Unlock()
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.storageMutex.Lock()
	if _, ok := ss.storage[args.Key]; !ok {
		// if key not found, create it
		ss.storage[args.Key] = []string{args.Value}
		reply.Status = storagerpc.OK
		ss.storageMutex.Unlock()
		return nil
	}
	l := ss.storage[args.Key].([]string)
	for _, val := range l {
		if val == args.Value {
			reply.Status = storagerpc.ItemExists
			ss.storageMutex.Unlock()
			return nil
		}
	}
	// if key found and value not exists
	l = append(l, args.Value)
	ss.storage[args.Key] = l
	reply.Status = storagerpc.OK
	ss.storageMutex.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.storageMutex.Lock()
	if l, ok := ss.storage[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		l := l.([]string)
		iToDelete := -1
		for i, val := range l {
			if val == args.Value {
				iToDelete = i
				break
			}
		}
		if iToDelete != -1 {
			l = append(l[:iToDelete], l[iToDelete+1:]...)
			ss.storage[args.Key] = l
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
	}
	ss.storageMutex.Unlock()
	return nil
}
