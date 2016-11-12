package storageserver

import (
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
	"strings"
	"hash/fnv"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type callbackWithID struct {
	valid bool
	ID int
	hostPort string
}

type callbacks struct {
	sync.Mutex
	cbID int
	list []*callbackWithID
}

type value struct {
	sync.RWMutex
	value interface{}
	callbacks callbacks
	revoking bool
	live bool
}

type storageServer struct {
	storage struct{
		sync.RWMutex
		m map[string]*value
	}
	servers []storagerpc.Node
	serversMutex *sync.Mutex
	numNodes int
	allRegisterNotify chan bool
	nodeID uint32
}


// StoreHash hashes a string key and returns a 32-bit integer. This function
// is provided here so that all implementations use the same hashing mechanism
// (both the Libstore and StorageServer should use this function to hash keys).
func StoreHash(key string) uint32 {
	prefix := strings.Split(key, ":")[0]
	hasher := fnv.New32()
	hasher.Write([]byte(prefix))
	return hasher.Sum32()
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
	ss.storage.m = make(map[string]*value)
	ss.serversMutex = &sync.Mutex{}
	ss.nodeID = nodeID
	if masterServerHostPort != "" {
		// This is a slave
		cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
		if err != nil {
			return nil, err
		}
		args := &storagerpc.RegisterArgs{
			ServerInfo: storagerpc.Node{
				HostPort: "127.0.0.1:" + strconv.Itoa(port),
				NodeID:   nodeID}}
		var reply storagerpc.RegisterReply
		for {
			err = cli.Call("StorageSrver.RegisterServer", args, &reply)
			if reply.Status == storagerpc.OK {
				ss.servers = reply.Servers
				break
			}
			time.Sleep(time.Second)
		}

		listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			return nil, err
		}
		err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
		if err != nil {
			return nil, err
		}
		rpc.HandleHTTP()
		go http.Serve(listener, nil)

	} else {
		// This is a master
		ss.servers = []storagerpc.Node{storagerpc.Node{
			HostPort: "127.0.0.1:" + strconv.Itoa(port),
			NodeID:   nodeID}}
		ss.numNodes = numNodes
		ss.allRegisterNotify = make(chan bool)

		listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		if err != nil {
			return nil, err
		}
		err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
		if err != nil {
			return nil, err
		}
		rpc.HandleHTTP()
		go http.Serve(listener, nil)

		<-ss.allRegisterNotify
	}
	return ss, nil
}

// Check if this server recieves correct key
func isCorrectNode(ss *storageServer, key string) bool {
	hash := StoreHash(key)
	var min uint32 = 4294967295
	var node storagerpc.Node = ss.servers[0]
	for _, item := range ss.servers {
		if item.NodeID-hash < min {
			node = item
			min = item.NodeID - hash
		}
	}
	if ss.nodeID == node.NodeID {
		return true
	} else {
		return false
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

func installLease(val *value, hostPort string) {
	leaseExpire := func(val *value, idToRemove int) {
		time.Sleep(time.Second*(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds))
		for _, cb := range val.callbacks.list {
			if cb.ID == idToRemove{
				cb.valid = false
				break
			}
		}
	}
	val.callbacks.Lock()
	val.callbacks.cbID += 1
	id := val.callbacks.cbID
	val.callbacks.list = append(val.callbacks.list, &callbackWithID{ID: id, hostPort: hostPort, valid: true})
	go leaseExpire(val, id)
	val.callbacks.Unlock()
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.storage.RLock()
	if val, ok := ss.storage.m[args.Key]; ok {
		ss.storage.RUnlock()
		val.RLock()
		if !val.live {
			reply.Status = storagerpc.KeyNotFound
		} else {
			reply.Status = storagerpc.OK
			if args.WantLease {
				if !val.revoking {
					installLease(val, args.HostPort)
					reply.Lease = storagerpc.Lease{
						Granted: true,
						ValidSeconds: storagerpc.LeaseSeconds}
				} else {
					reply.Lease = storagerpc.Lease{Granted: false}
				}
			}
			reply.Value = val.value.(string)
		}
		val.RUnlock()
	} else {
		ss.storage.RUnlock()
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func revokeLeases(val *value, key string) {
	val.callbacks.Lock()
	revoked := make(chan bool)
	for _, cb := range val.callbacks.list {
		go func(cb *callbackWithID) {
			done := make(chan *rpc.Call, 1)
			cli, _ := rpc.DialHTTP("tcp", cb.hostPort)
			args := &storagerpc.RevokeLeaseArgs{}
			args.Key = key
			var reply storagerpc.RevokeLeaseReply
			exitFor := false
			for ;cb.valid && !exitFor ; {
				cli.Go("LeaseCallbacks.RevokeLease", args, &reply, done)
				select {
				case <- time.After(time.Second):
				case <- done:
					exitFor = true
				}
			}
			revoked <- true
		}(cb)
	}

	// Wait till all callbacks are revoked or expired
	for _ = range val.callbacks.list {
		<- revoked
	}

	val.callbacks.list = make([]*callbackWithID, 0)
	val.callbacks.Unlock()
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}


	ss.storage.RLock()
	if val, ok := ss.storage.m[args.Key]; ok {
		ss.storage.RUnlock()
		val.Lock()
		val.revoking = true
		val.Unlock()

		revokeLeases(val, args.Key)//block; not locked, so can be read

		val.Lock()
		val.value = args.Value
		val.revoking = false
		val.Unlock()
	} else {
		ss.storage.RUnlock()
		ss.storage.Lock()
		ss.storage.m[args.Key] = &value{
			value: args.Value,
			callbacks: callbacks{cbID: 0, list: make([]*callbackWithID, 0)},
			live: true,
			revoking: false}
		ss.storage.Unlock()
	}
	reply.Status = storagerpc.OK

	return nil
}

func revokeLeases(ss *storageServer, key string) error {
	ss.storage[key].callbacks.Lock()
	for _, cb := range ss.storage[key].callbacks.list {
		cli, err := rpc.DialHTTP("tcp", cb.hostPort)
		if err != nil {
			ss.storage[key].callbacks.Unlock()
			return err
		}
		args := &storagerpc.RevokeLeaseArgs{}
		args.Key = key
		var reply storagerpc.RevokeLeaseReply
		err = cli.Call("LeaseCallbacks.RevokeLease", args, &reply)
		if err != nil {
			ss.storage[key].callbacks.Unlock()
			return err
		}
	}
	ss.storage[key].callbacks.list = make([]callbackWithID,0)
	ss.storage[key].callbacks.Unlock()
	return nil
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storage.RLock()
	if val, ok := ss.storage.m[args.Key]; ok {
		ss.storage.RUnlock()
		val.Lock()
		val.revoking = true
		val.Unlock()

		revokeLeases(val, args.Key)// not locked, so can be read

		val.Lock()
		ss.storage.Lock()
		val.live = false
		delete(ss.storage.m, args.Key)
		ss.storage.Unlock()
		val.Unlock()
		reply.Status = storagerpc.OK
	} else {
		ss.storage.RUnlock()
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}
	ss.storage.RLock()
	if val, ok := ss.storage.m[args.Key]; ok {
		ss.storage.RUnlock()
		val.RLock()
		if !val.live {
			reply.Status = storagerpc.KeyNotFound
		} else {
			reply.Status = storagerpc.OK
			if args.WantLease {
				if !val.revoking {
					installLease(val, args.HostPort)
					reply.Lease = storagerpc.Lease{
						Granted: true,
						ValidSeconds: storagerpc.LeaseSeconds}
				} else {
					reply.Lease = storagerpc.Lease{Granted: false}
				}
			}
			reply.Value = val.value.([]string)
		}
		val.RUnlock()
	} else {
		ss.storage.RUnlock()
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}


	ss.storage.RLock()
	if val, ok := ss.storage.m[args.Key]; ok {
		ss.storage.RUnlock()
		val.Lock()
		val.revoking = true
		val.Unlock()

		revokeLeases(val, args.Key)//block; not locked, so can be read

		val.Lock()
		alreadyExist := false
		for _, v := range(val.value.([]string)) {
			if v == args.Value {
				alreadyExist = true
				break
			}
		}
		if alreadyExist {
			reply.Status = storagerpc.ItemExists
		} else {
			val.value = append(val.value.([]string), args.Value)
			reply.Status = storagerpc.OK
		}
		val.revoking = false
		val.Unlock()
	} else {
		ss.storage.RUnlock()
		ss.storage.Lock()
		ss.storage.m[args.Key] = &value{
			value: []string{args.Value},
			callbacks: callbacks{cbID: 0, list: make([]*callbackWithID, 0)},
			live: true,
			revoking: false}
		ss.storage.Unlock()
		reply.Status = storagerpc.OK
	}


	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storage.RLock()
	if val, ok := ss.storage.m[args.Key]; ok {
		ss.storage.RUnlock()
		val.Lock()
		val.revoking = true
		val.Unlock()

		revokeLeases(val, args.Key)// not locked, so can be read

		val.Lock()

		iToRemove := -1
		for i, v := range(val.value.([]string)) {
			if v == args.Value {
				iToRemove = i
				break
			}
		}

		if iToRemove != -1 {
			l := val.value.([]string)
			val.value = append(l[:iToRemove], l[iToRemove+1:]...)
			if len(val.value.([]string)) == 0 {
				ss.storage.Lock()
				val.live = false
				delete(ss.storage.m, args.Key)
				ss.storage.Unlock()
			}
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}

		val.revoking = false
		val.Unlock()
	} else {
		ss.storage.RUnlock()
		reply.Status = storagerpc.KeyNotFound
	}

	return nil
}
