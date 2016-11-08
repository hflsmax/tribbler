package storageserver

import (
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"time"
	"strings"
	"hash/fnv"
	"fmt"
)

type callbackWithID struct {
	ID int
	hostPort string
}

type callbacks struct{
	sync.Mutex
	cbID int
	list []callbackWithID
}

type value struct {
	// sync.RWMutex
	value interface{}
	callbacks *callbacks
	// live bool
}

type storageServer struct {
	storage           map[string]value
	storageMutex      *sync.Mutex
	servers           []storagerpc.Node
	serversMutex      *sync.Mutex
	numNodes          int
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
	ss.storage = make(map[string]value)
	ss.storageMutex = &sync.Mutex{}
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
				return ss, nil
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

func installLease(ss *storageServer, key string, hostPort string) {
	ss.storage[key].callbacks.Lock()
	ss.storage[key].callbacks.cbID += 1
	ss.storage[key].callbacks.list = append(ss.storage[key].callbacks.list, callbackWithID{ID: ss.storage[key].callbacks.cbID, hostPort: hostPort})
	fmt.Println("install lease: ", key, hostPort)
	ss.storage[key].callbacks.Unlock()


	// Remove callback after a predetermined time
	go func(hostPort string, idToRemove int) {
		time.Sleep(time.Second*(storagerpc.LeaseSeconds+storagerpc.LeaseGuardSeconds))
		fmt.Println("remove callback", hostPort)
		ss.storage[key].callbacks.Lock()
		toRemove := -1
		for i, h := range ss.storage[key].callbacks.list {
			if h.ID ==  idToRemove{
				toRemove = i
				break
			}
		}
		if toRemove != -1 {
			ss.storage[key].callbacks.list =
				append(ss.storage[key].callbacks.list[:toRemove], ss.storage[key].callbacks.list[toRemove+1:]...)
		}
		ss.storage[key].callbacks.Unlock()
	}(hostPort, ss.storage[key].callbacks.cbID)
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storageMutex.Lock()
	if val, ok := ss.storage[args.Key]; ok {
		// if !val.live {
		// 	// value has been deleted
		// 	reply.Status = storagerpc.KeyNotFound
		// } else {
		if args.WantLease {
			installLease(ss, args.Key, args.HostPort)
			reply.Lease = storagerpc.Lease{
				Granted: true,
				ValidSeconds: storagerpc.LeaseSeconds}
		}
		reply.Status = storagerpc.OK
		fmt.Println("get", args.Key, ":", val.value.(string))
		reply.Value = val.value.(string)
		// }
	} else {
		fmt.Println("get", args.Key, ":", "keynotfound")
		reply.Status = storagerpc.KeyNotFound
	}
	ss.storageMutex.Unlock()
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

	ss.storageMutex.Lock()
	if _, ok := ss.storage[args.Key]; ok {
		if err := revokeLeases(ss, args.Key); err != nil {
			ss.storageMutex.Unlock()
			return err
		}
		// ss.storage[args.Key].live = false
		delete(ss.storage, args.Key)
		fmt.Println("delete ", args.Key)
		reply.Status = storagerpc.OK
	} else {
		fmt.Println("delete ", args.Key, " KeyNotFound")
		reply.Status = storagerpc.KeyNotFound
	}
	ss.storageMutex.Unlock()
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storageMutex.Lock()
	if val, ok := ss.storage[args.Key]; ok {
		// if !val.live {
		// 	//value has been deleted
		// 	reply.Status = storagerpc.KeyNotFound
		// } else {
		if args.WantLease {
			installLease(ss, args.Key, args.HostPort)
			reply.Lease = storagerpc.Lease{
				Granted: true,
				ValidSeconds: storagerpc.LeaseSeconds}
		}
		reply.Status = storagerpc.OK
		fmt.Println("getlist ", args.Key, ": ", val.value.([]string))
		reply.Value = val.value.([]string)
		// }
	} else {
		fmt.Println("getlist ", args.Key, ": KeyNotFound")
		reply.Status = storagerpc.KeyNotFound
	}
	ss.storageMutex.Unlock()
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storageMutex.Lock()
	if _, ok := ss.storage[args.Key]; ok {
		if err := revokeLeases(ss, args.Key); err != nil {
			ss.storageMutex.Unlock()
			return err
		}
		fmt.Println("put update", args.Key, ":", args.Value)
		ss.storage[args.Key] = value{value: args.Value, callbacks: &callbacks{cbID: 0, list: make([]callbackWithID,0)}}
	} else {
		fmt.Println("put", args.Key, ":", args.Value)
		ss.storage[args.Key] = value{
			callbacks: &callbacks{cbID: 0, list: make([]callbackWithID, 0)},
			value: args.Value}
	}
	ss.storageMutex.Unlock()
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storageMutex.Lock()
	if _, ok := ss.storage[args.Key]; !ok {
		// if key not found, create it
		ss.storage[args.Key] = value{
			callbacks: &callbacks{cbID: 0, list: make([]callbackWithID, 0)},
			value: []string{args.Value}}
		reply.Status = storagerpc.OK
		ss.storageMutex.Unlock()
		return nil
	}
	l := ss.storage[args.Key].value.([]string)
	for _, val := range l {
		if val == args.Value {
			reply.Status = storagerpc.ItemExists
			ss.storageMutex.Unlock()
			return nil
		}
	}
	// if key found and value not exists
	l = append(l, args.Value)
	if err := revokeLeases(ss, args.Key); err != nil {
		ss.storageMutex.Unlock()
		return err
	}
	ss.storage[args.Key] = value{value: l, callbacks: &callbacks{cbID: 0, list: make([]callbackWithID,0)}}
	reply.Status = storagerpc.OK
	ss.storageMutex.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if !isCorrectNode(ss, args.Key) {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storageMutex.Lock()
	if l, ok := ss.storage[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
	} else {
		l := l.value.([]string)
		iToDelete := -1
		for i, val := range l {
			if val == args.Value {
				iToDelete = i
				break
			}
		}
		if iToDelete != -1 {
			l = append(l[:iToDelete], l[iToDelete+1:]...)
			if err := revokeLeases(ss, args.Key); err != nil {
				ss.storageMutex.Unlock()
				return err
			}
			ss.storage[args.Key] = value{value: l, callbacks: &callbacks{cbID: 0, list: make([]callbackWithID,0)}}
			reply.Status = storagerpc.OK
		} else {
			reply.Status = storagerpc.ItemNotFound
		}
	}
	ss.storageMutex.Unlock()
	return nil
}
