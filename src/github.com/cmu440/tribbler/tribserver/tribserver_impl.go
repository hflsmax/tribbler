package tribserver

import (

	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"  
	"github.com/cmu440/tribbler/libstore"                
    "net/rpc"
	"net/http"  
	"net"                                                                                                                                                                                                                                                                                                       
	"time"
	"encoding/json"
	"bytes"
	"fmt"
)

type tribServer struct {
	// TODO: implement this!
	ls libstore.Libstore
	postTime int64
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
    tribServer := new(tribServer)
    ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
    if err != nil {
    	return nil, err
    }
    tribServer.ls = ls
    tribServer.postTime = 0
    // Create the server socket that will listen for incoming RPCs.
    listener, err := net.Listen("tcp", fmt.Sprintf(":%d", myHostPort))
    if err != nil {
        return nil, err
    }

    // Wrap the tribServer before registering it for RPC.
    err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
    if err != nil {
        return nil, err
    }

    // Setup the HTTP handler that will server incoming RPCs and
    // serve requests in a background goroutine.
    rpc.HandleHTTP()
    go http.Serve(listener, nil)

    return tribServer, nil
}

//map user to its list of friends
func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	s := util.FormatUserKey(args.UserID)
	_, err := ts.ls.GetList(s)	
	if err == nil {
		reply.Status = tribrpc.Exists
		return nil
	} else {
		err = ts.ls.AppendToList(s, "")
		reply.Status = tribrpc.OK
		return err 
	}
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	id := util.FormatUserKey(args.UserID)
	tid := util.FormatUserKey(args.TargetUserID)
	_, err1 := ts.ls.GetList(id)
	_, err2 := ts.ls.GetList(tid)
	var err error 
	if err1 != nil {
		reply.Status = tribrpc.NoSuchUser
	} else if err2 != nil {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		lid := util.FormatSubListKey(args.UserID)
		//add and then put 
		l, _ := ts.ls.GetList(lid)
		for _, str := range l {
			if bytes.Compare([]byte(str), []byte(args.TargetUserID)) == 0 {
				reply.Status = tribrpc.Exists
				return err 
			}
		}
		err = ts.ls.AppendToList(lid, args.TargetUserID)
		reply.Status = tribrpc.OK
	}	
	return err
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	id := util.FormatUserKey(args.UserID)
	tid := util.FormatUserKey(args.TargetUserID)
	var err error 
	_, err1 := ts.ls.GetList(id)
	_, err2 := ts.ls.GetList(tid)
	if err1 != nil {
		reply.Status = tribrpc.NoSuchUser
	} else if err2 != nil {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		lid := util.FormatSubListKey(args.UserID)
		//remove and then put 
		l, _ := ts.ls.GetList(lid)
		for _, str := range l {
			if bytes.Compare([]byte(str), []byte(args.TargetUserID)) == 0 {
				reply.Status = tribrpc.OK 
				err := ts.ls.RemoveFromList(lid, args.TargetUserID)
				return err 
			}
		}
		reply.Status = tribrpc.NoSuchTargetUser
	}
	return err 
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {

	id := util.FormatUserKey(args.UserID)
	l, err := ts.ls.GetList(id)
	reply.Status = tribrpc.OK
	reply.UserIDs = l
	return err
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	reply.PostKey = util.FormatPostKey(args.UserID, ts.postTime)
	ts.postTime += 1
	_, err := ts.ls.Get(reply.PostKey)
	for err == nil {
		reply.PostKey = util.FormatPostKey(args.UserID, ts.postTime)
		ts.postTime += 1
		_, err := ts.ls.Get(reply.PostKey)
	}
	t := new(tribrpc.Tribble)
	t.UserID = args.UserID 
	t.Posted = time.Now()
	t.Contents = args.Contents 
	b, _ := json.Marshal(t) 
	err := ts.ls.Put(reply.PostKey, string(b))
	reply.Status = tribrpc.OK
	return err 
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	err := ts.ls.Delete(args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
	} else {
		reply.Status = tribrpc.OK
	}
	return err 
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	k := util.FormatTribListKey(args.UserID)
	l, err := ts.ls.GetList(k)
	if (err != nil) {
		return err
	}
	tribs := make([]tribrpc.Tribble, 100)
	i := 0
	for (i < len(l) && i < 100) {
		var t tribrpc.Tribble 
		s, err := ts.ls.Get(l[i])
		if err != nil {
			json.Unmarshal([]byte(s), &t)
			tribs[i] = t 
			i += 1
		}
	}
	reply.Tribbles = tribs
	reply.Status = tribrpc.OK
	return err
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	k := util.FormatSubListKey(args.UserID)
	l, err := ts.ls.GetList(k)
	if (err != nil){
		return err 
	}
	tribs := make([]tribrpc.Tribble, 100)
	latest := make([]tribrpc.Tribble, len(l))
	tribLists := make([][]string, len(l))
	nonempty := 0
	j := 0
	for (j < len(l)){
		k := util.FormatTribListKey(l[j])
		tl, err := ts.ls.GetList(k)
		tribLists[j] = tl
		if err == nil && len(tl) > 0{
			nonempty += 1 
			var t tribrpc.Tribble
			s, err := ts.ls.Get(tl[0])
			if err != nil {
				json.Unmarshal([]byte(s), &t)
				latest[j] = t
				tribLists[j] = tl[1:]
			}
		}
		j += 1
	}
	i := 0
	for (i < 100 && nonempty > 0){
		var min tribrpc.Tribble
		index := 0
		j = 0
		for (j < len(latest)) {
			t := latest[j]
			if len(t.UserID) != 0 && len(min.UserID) == 0 {
				min = t 
				index = j
				continue 
			}
			if len(t.UserID) != 0 && t.Posted.After(min.Posted) {
				min = t 
				index = j
			}
			j += 1
		}
		tribs[i] = min 
		//replace the index-th 
		if (len(tribLists[index]) > 0){
			var t tribrpc.Tribble
			k := 0
			found := false 
			for len(tribLists[index]) > 0{
				s, err := ts.ls.Get(tribLists[index][k])
				if err != nil {
					json.Unmarshal([]byte(s), &t)
					latest[index] = t
					tribLists[index] = tribLists[index][k+1:]
					found = true 
					break
				}
				k += 1
			}
			if found == false {
				nonempty -= 1
			}
		} else {
			nonempty -= 1
		}
		i += 1
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribs
	return nil
}
