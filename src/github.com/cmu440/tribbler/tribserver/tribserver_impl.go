package tribserver

import (
	"bytes"
	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"time"
	//"fmt"
)

type tribServer struct {
	ls       libstore.Libstore
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
	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		return nil, err
	}
	tribServer.ls = ls
	tribServer.postTime = 0
	// Create the server socket that will listen for incoming RPCs.
	listener, err := net.Listen("tcp", myHostPort)
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
		if err != nil {
			reply.Status = tribrpc.Exists
		} else {
			reply.Status = tribrpc.OK
		}
		return nil
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
		lid2 := util.FormatSubListKey(args.TargetUserID)
		l4, _ := ts.ls.GetList(lid2)
		for _, str := range l4 {
			if bytes.Compare([]byte(str), []byte(args.UserID)) == 0 {
				ts.ls.AppendToList(id, args.TargetUserID)
				ts.ls.AppendToList(tid, args.UserID)
				break
			}
		}
	}
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	id := util.FormatUserKey(args.UserID)
	tid := util.FormatUserKey(args.TargetUserID)
	var err error
	_, err1 := ts.ls.GetList(id)
	l2, err2 := ts.ls.GetList(tid)
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
				ts.ls.RemoveFromList(lid, args.TargetUserID)
				for _, str2 := range l2 {
					if bytes.Compare([]byte(str2), []byte(args.UserID)) == 0 {
						ts.ls.RemoveFromList(tid, args.UserID)
						ts.ls.RemoveFromList(id, args.TargetUserID)
						break
					}
				}
				return nil
			}
		}
		reply.Status = tribrpc.NoSuchTargetUser
	}
	return err
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {

	id := util.FormatUserKey(args.UserID)
	l, err := ts.ls.GetList(id)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	} else {
		reply.Status = tribrpc.OK
		reply.UserIDs = l[1:]
	}
	return err
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	reply.PostKey = util.FormatPostKey(args.UserID, ts.postTime)
	ts.postTime += 1
	_, err := ts.ls.Get(reply.PostKey)
	for err == nil {
		reply.PostKey = util.FormatPostKey(args.UserID, ts.postTime)
		ts.postTime += 1
		_, err = ts.ls.Get(reply.PostKey)
	}
	_, err = ts.ls.GetList(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	t := new(tribrpc.Tribble)
	t.UserID = args.UserID
	t.Posted = time.Now()
	t.Contents = args.Contents
	b, _ := json.Marshal(t)
	err = ts.ls.Put(reply.PostKey, string(b))
	ts.ls.AppendToList(util.FormatTribListKey(args.UserID), reply.PostKey)
	reply.Status = tribrpc.OK
	return err
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	_, err2 := ts.ls.GetList(util.FormatUserKey(args.UserID))
	if err2 != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err3 := ts.ls.Get(args.PostKey)
	if err3 != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	err := ts.ls.Delete(args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	} else {
		reply.Status = tribrpc.OK
		ts.ls.RemoveFromList(util.FormatTribListKey(args.UserID), args.PostKey)
	}
	return err
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_, err1 := ts.ls.GetList(util.FormatUserKey(args.UserID))
	if err1 != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	k := util.FormatTribListKey(args.UserID)
	l, err := ts.ls.GetList(k)
	if len(l) == 0 {
		reply.Tribbles = nil
		reply.Status = tribrpc.OK
		return nil
	}
	tribs := sortList(l, ts)
	reply.Tribbles = tribs
	reply.Status = tribrpc.OK
	return err
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	_, err1 := ts.ls.GetList(util.FormatUserKey(args.UserID))
	if err1 != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	k := util.FormatSubListKey(args.UserID)
	l, err := ts.ls.GetList(k)
	if err != nil {
		reply.Status = tribrpc.OK
		return nil
	}
	tribs := make([]tribrpc.Tribble, 100)
	latest := make([]tribrpc.Tribble, len(l))
	tribLists := make([][]tribrpc.Tribble, len(l))
	nonempty := 0
	j := 0
	for j < len(l) {
		k := util.FormatTribListKey(l[j])
		tl, err := ts.ls.GetList(k)
		tribLists[j] = sortList(tl, ts)
		if err == nil && len(tribLists[j]) > 0 {
			nonempty += 1
			latest[j] = tribLists[j][0]
			tribLists[j] = tribLists[j][1:]
		}
		j += 1
	}
	i := 0
	for i < 100 && nonempty > 0 {
		var min tribrpc.Tribble
		index := 0
		j = 0
		for j < len(latest) {
			t := latest[j]
			if len(t.UserID) != 0 && (len(min.UserID) == 0 || t.Posted.After(min.Posted)) {
				min = t
				index = j
			}
			j += 1
		}
		tribs[i] = min
		//replace the index-th
		if len(tribLists[index]) > 0 {
			latest[index] = tribLists[index][0]
			tribLists[index] = tribLists[index][1:]
		} else {
			latest[index].UserID = ""
			nonempty -= 1
		}
		i += 1
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribs[:i]
	return nil
}

func sortList(l []string, ts *tribServer) []tribrpc.Tribble {
	tribs := make([]tribrpc.Tribble, 100)
	i := 0
	//sort l
	for s1, t1 := range l {
		var minT tribrpc.Tribble
		s, err := ts.ls.Get(t1)
		if err == nil {
			json.Unmarshal([]byte(s), &minT)
		} else {
			continue
		}
		index := s1
		for s2, t2 := range l {
			if s2 < s1 {
				continue
			}
			var t tribrpc.Tribble
			s, err := ts.ls.Get(t2)
			if err == nil {
				json.Unmarshal([]byte(s), &t)
				if t.Posted.After(minT.Posted) {
					minT = t
					index = s2
				}
			}
		}
		tribs[i] = minT
		temp := l[index]
		l[index] = l[s1]
		l[s1] = temp
		i += 1
		if i >= 100 {
			break
		}
	}
	return tribs[:i]

}
