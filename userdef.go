package main

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

/* In this file, you should implement function "NewNode" and
 * a struct which implements the interface "dhtNode".
 */

func NewNode(port int) dhtNode {
	// Todo: create a node and then return it.
	var re *Node = new(Node)
	re.Address = get_ip()
	re.Port = fmt.Sprintf("%v", port)
	re.data = make(map[string]string)
	re.sv = make(map[string]string)
	re.id = get_hash(fmt.Sprintf("%v:%v", re.Address, port))
	return re
}

// Todo: implement a struct which implements the interface "dhtNode".
const M int = 3
const _ti time.Duration = 60 * time.Millisecond

type Node struct {
	Address  string
	Port     string
	pre      string
	suc      string
	suc_list [M]string
	finger   [161]string
	data     map[string]string
	sv       map[string]string
	id       *big.Int
	next     int
	myServer *Serv
	exit     chan bool
}

type Serv struct {
	listener net.Listener
	listen   bool
	node     *Node
	Server   *rpc.Server
}

type Pair struct {
	Key string
	Val string
}

var mp map[string]int = make(map[string]int)

func make_rpc_address(addr string) string {
	var res int = mp[addr] + 1
	mp[addr] = res
	return fmt.Sprintf("/%v_%v", addr, res)
}

func rpc_address(addr string) string {
	var res int = mp[addr]
	return fmt.Sprintf("/%v_%v", addr, res)
}

func (s *Serv) info() string {
	return fmt.Sprintf("ID: %v\nAddress: %v:%v\nData: %v\nSuccessor: %v\nPredecessor: %v\nFingers: %v", s.node.id, s.node.Address, s.node.Port, s.node.data, s.node.suc, s.node.pre, s.node.finger[1:])
}

func (nd *Node) get_address() string {
	return fmt.Sprintf("%v:%v", nd.Address, nd.Port)
}

func (nd *Node) get_suc() string {
	//return nd.suc

	var ok bool = false
	for ti := 0; ti < 2; ti++ {
		ok = nd.Ping(nd.suc)
		if ok {
			return nd.suc
		}
	}

	fmt.Printf("[%v] suc <%v> failed\n", nd.get_address(), nd.suc)

	var i int
	for i = 0; i != M; i++ {
		ok = nd.Ping(nd.suc_list[i])
		if ok {
			fmt.Printf("[%v] suc change: from %v to %v <fail>\n", nd.get_address(), nd.suc, nd.suc_list[i])
			nd.suc = nd.suc_list[i]
			for ii := 0; ii+i < M; ii++ {
				nd.suc_list[ii] = nd.suc_list[ii+i]
			}
			return nd.suc
		}
	}
	nd.suc = nd.get_address()
	return nd.get_address()
}

func (nd *Node) Run() {
	nd.myServer = &Serv{
		node: nd,
	}
	nd.myServer.Listen()
}

var set_log bool = false

func (server *Serv) Listen() {
	server.Server = rpc.NewServer()
	if !set_log {
		f, _ := os.OpenFile("/log/log.txt", os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0755)
		log.SetOutput(f)
		set_log = true
	}
	server.Server.Register(server.node)
	addr := server.node.get_address()
	fmt.Printf("[%v] register\n", addr)

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	server.node.exit = make(chan bool)
	server.listen = true
	server.listener = listener
	rpc_path := make_rpc_address(addr)
	server.Server.HandleHTTP(rpc_path, rpc_path+"debug")
	go http.Serve(listener, nil)
}
func betw(x *big.Int, L *big.Int, R *big.Int) bool {
	if R.Cmp(L) == 1 {
		return L.Cmp(x) < 0 && x.Cmp(R) < 0
	}
	return L.Cmp(x) < 0 || x.Cmp(R) < 0
}

func (nd *Node) Get_list(tmp int, res *[M]string) error {
	for i := 0; i < M; i++ {
		res[i] = nd.suc_list[i]
	}
	return nil
}

func (nd *Node) Notify_rpc(tar string, resp *bool) error {
	//	fmt.Printf("[%v]:rpc check pre <%v>\n", nd.get_address(), tar)
	if nd.pre == "" || betw(get_hash(tar), get_hash(nd.pre), nd.id) {
		nd.pre = tar
	}
	return nil
}

func Notify(cur string, tar string) error {
	//	fmt.Printf("[%v]:check pre <%v>\n", cur, tar)
	if cur == "" {
		return errors.New("Notify: empty address")
	}
	client, _ := dial(cur)
	if client == nil {
		return errors.New("Notify: empty client")
	}
	defer client.Close()
	var resp bool
	return client.Call("Node.Notify_rpc", tar, &resp)
}

func (nd *Node) stabilize() bool {
	if !nd.myServer.listen {
		return false
	}
	p, e := FindPredecessor(nd.get_suc())
	if e == nil {
		if betw(get_hash(p), nd.id, get_hash(nd.suc)) {

			fmt.Printf("[%v] suc change: from %v to %v\n", nd.get_address(), nd.suc, p)

			nd.suc = p
		}
	}
	Notify(nd.suc, nd.get_address())

	client, err := dial(nd.suc)

	//	fmt.Printf("[%v] pre:%v suc: %v\n", nd.get_address(), nd.pre, nd.suc)

	err = client.Call("Node.Get_list", 0, &nd.suc_list)
	if err != nil {
		fmt.Println(err)
	}

	for i := M - 1; i > 0; i-- {
		nd.suc_list[i] = nd.suc_list[i-1]
	}
	nd.suc_list[0] = nd.suc
	client.Close()
	return true
}
func (nd *Node) _stabilize() {
	t := time.NewTicker(_ti)
	//t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-nd.exit:
			//	fmt.Println("exit stabilize")
			return
		case <-t.C:
			ok := nd.stabilize()
			if !ok {
				//		fmt.Println("exit stabilize")
				return
			}
		}
	}
}

func (nd *Node) check_pre() bool {
	if !nd.myServer.listen {
		return false
	}
	if nd.pre != "" {
		ok := nd.Ping(nd.pre)
		if !ok {
			fmt.Printf("[%v] pre <%v> has failed\n", nd.get_address(), nd.pre)
			nd.get_sv()
			nd.pre = ""
		}
	}
	return true
}
func (nd *Node) _check_pre() {
	t := time.NewTicker(_ti)
	//t := time.NewTicker(time.Second)
	for {
		select {
		case <-nd.exit:
			//	fmt.Println("exit check_pre")
			return
		case <-t.C:
			ok := nd.check_pre()
			if !ok {
				//	fmt.Println("exit check_pre")
				return
			}
		}
	}
}

func (nd *Node) fix_fingers() bool {
	if !nd.myServer.listen {
		return false
	}
	nd.next++
	if nd.next > 160 {
		nd.next = 1
	}
	var res string = ""
	tmp := get_hash(nd.get_address())
	err := nd.FindSuccessor(calc(tmp, nd.next), &res)
	if err != nil {
		fmt.Println(err)
	}
	nd.finger[nd.next] = res
	if nd.next%40 == 0 {
		nd.save_data()
	}
	return true
}

func (nd *Node) _fix_fingers() {
	t := time.NewTicker(_ti)
	//t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-nd.exit:
			//	fmt.Println("exit fix_fingers")
			return
		case <-t.C:
			ok := nd.fix_fingers()
			if !ok {
				//		fmt.Println("exit fix_fingers")
				return
			}
		}
	}
}

func (nd *Node) Create() {
	fmt.Printf("Node created at %v:%v\n", nd.Address, nd.Port)

	nd.pre = ""
	nd.suc = nd.get_address()
	nd.suc_list[0] = nd.suc

	go nd._stabilize()
	go nd._check_pre()
	go nd._fix_fingers()
}

func (nd *Node) Join(addr string) bool {

	fmt.Printf("[%v] join at %v\n", nd.get_address(), addr)

	id := get_hash(fmt.Sprintf("%v:%v", nd.Address, nd.Port))

	client, err := dial(addr)

	if err != nil {
		fmt.Printf("error: client was nil")
		os.Exit(2)
	}

	defer client.Close()
	var resp string

	err = client.Call("Node.FindSuccessor", id, &resp)

	if err != nil {
		fmt.Printf("Error in join %v\n", err)
		return false
	}
	nd.pre = ""
	nd.suc = resp
	nd.suc_list[0] = nd.suc
	cli2, _ := dial(nd.suc)
	defer cli2.Close()
	cli2.Call("Node.Update", "", &resp)

	go nd._stabilize()
	go nd._check_pre()
	go nd._fix_fingers()

	return true
}

func (nd *Node) Ping(addr string) bool {
	if addr == "" {
		return false
	}
	client, err := dial(addr)
	if err != nil {
		return false
	}
	defer client.Close()
	var res int = 0
	err = client.Call("Node.Ping_rpc", 0, &res)
	if err != nil || res != 1 {
		return false
	}
	return true
}

func (nd *Node) Put(key string, value string) bool {
	tar := nd.find(key)
	client, e := dial(tar)
	if e != nil {
		return false
	}
	defer client.Close()
	da := Pair{key, value}
	var res bool
	e = client.Call("Node.Put_rpc", da, &res)
	if e != nil {
		panic(e)
		return false
	}
	//	fmt.Printf("[%v] stored %v at %v\n", tar, da.Val, da.Key)
	return true
}

func (nd *Node) Put_rpc(da Pair, res *bool) error {
	nd.data[da.Key] = da.Val
	return nil
}

func (nd *Node) Clear_sv(tmp int, res *bool) error {
	for k, _ := range nd.sv {
		delete(nd.sv, k)
	}
	return nil
}

func (nd *Node) Put_sv(da Pair, res *bool) error {
	nd.sv[da.Key] = da.Val
	return nil
}

func (nd *Node) get_sv() {
	for k, v := range nd.sv {
		nd.data[k] = v
		delete(nd.sv, k)
	}
}

func (nd *Node) Get(key string) (bool, string) {
	tar := nd.find(key)
	client, e := dial(tar)
	if e != nil {
		fmt.Printf("error in get : %v\n", e)
		return false, ""
	}
	defer client.Close()
	var res string
	e = client.Call("Node.Get_rpc", key, &res)
	if e != nil {
		//		fmt.Printf("error in get : %v\n", e)
		return false, ""
	}
	//	fmt.Printf("[%v] : %v\n", tar, res)
	return true, res
}

func (nd *Node) Ping_rpc(tmp int, res *int) error {
	*res = 1
	return nil
}

func (nd *Node) Get_rpc(key string, res *string) error {
	resp := nd.data[key]
	if resp == "" {
		resp = nd.sv[key]
		if resp == "" {
			return errors.New("not found.")
		}
	}
	*res = resp
	return nil
}

func (nd *Node) Delete(key string) bool {
	tar := nd.find(key)
	client, e := dial(tar)
	if e != nil {
		return false
	}
	defer client.Close()
	var res bool
	e = client.Call("Node.Delete_rpc", key, &res)
	if e != nil {
		panic(e)
		return false
	}
	//	fmt.Printf("[%v] deleted key %v\n", tar, key)
	return true
}

func (nd *Node) Delete_rpc(key string, res *bool) error {
	delete(nd.data, key)
	return nil
}

func (nd *Node) Set_suc(addr string, res *bool) error {
	nd.suc = addr
	nd.suc_list[0] = nd.suc
	return nil
}

func (nd *Node) Set_pre(addr string, res *bool) error {
	nd.pre = addr
	return nil
}

func dial(address string) (*rpc.Client, error) {
	//	fmt.Printf("dial <%v>\n", address)
	res, err := rpc.DialHTTPPath("tcp", address, rpc_address(address))
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (nd *Node) Update(tmp string, res *string) error {
	go nd.upd_data()
	return nil
}

func (nd *Node) upd_data() error {
	//time.Sleep(3 * time.Second)
	time.Sleep(3 * _ti)
	var mp map[string]string = make(map[string]string)
	for k, v := range nd.data {
		mp[k] = v
		delete(nd.data, k)
	}
	for k, v := range mp {
		nd.Put(k, v)
	}
	return nil
}

func (nd *Node) save_data() error {
	//	fmt.Printf("[%v]: save_data\n", nd.get_address())
	tar := nd.get_suc()
	client, e := dial(tar)
	if e != nil {
		return e
	}
	defer client.Close()
	var res bool
	e = client.Call("Node.Clear_sv", 0, &res)
	if e != nil {
		return e
	}
	for k, v := range nd.data {
		da := Pair{k, v}
		e = client.Call("Node.Put_sv", da, &res)
	}
	return nil
}

func (nd *Node) Get_pre(tmp string, res *string) error {
	ok := nd.Ping(nd.pre)
	if ok {
		*res = nd.pre
		return nil
	}
	*res = ""
	return errors.New("")
}

func FindPredecessor(addr string) (string, error) {
	if addr == "" {
		return "", errors.New("FindPredecessor: rpc address was empty")
	}
	client, _ := dial(addr)
	if client == nil {
		return "", errors.New("FindPredecessor: Client was nil")
	}
	defer client.Close()
	var res string
	err := client.Call("Node.Get_pre", "", &res)
	if err != nil {
		return "", err
	}
	if res == "" {
		return "", errors.New("Empty predecessor")
	}
	return res, nil
}

func (nd *Node) closest_preceding_node(id *big.Int) string {
	for i := 160; i >= 1; i-- {
		if nd.finger[i] != "" && betw(get_hash(nd.finger[i]), nd.id, id) {
			ok := nd.Ping(nd.finger[i])
			if ok {
				return nd.finger[i]
			}
			return nd.get_suc()
		}
	}
	return nd.get_suc()
}

func (nd *Node) FindSuccessor(id *big.Int, res *string) error {
	//	fmt.Printf("%v Find_suc\n", nd.get_address())

	var L, R *big.Int = nd.id, get_hash(nd.get_suc())

	if betw(id, L, R) || id.Cmp(R) == 0 {
		*res = nd.suc
		return nil
	}
	//	fmt.Printf("%v Find_suc set client\n", nd.get_address())
	client, err := dial(nd.closest_preceding_node(id))
	defer client.Close()
	var resp string
	err = client.Call("Node.FindSuccessor", id, &resp)
	*res = resp
	if err != nil {
		return err
	}
	return nil
}

func (nd *Node) find(key string) string {
	client, err := dial(nd.get_address())
	if err != nil {
		fmt.Println(err)
		return ""
	}
	defer client.Close()
	var res string
	e := client.Call("Node.FindSuccessor", get_hash(key), &res)
	if e != nil {
		fmt.Println(e)
		return ""
	}
	return res
}

func (nd *Node) Quit() {
	if !nd.myServer.listen {
		return
	}
	addr := nd.get_address()
	fmt.Printf("[%v] quit\n", addr)
	for {
		nd.get_suc()
		fmt.Printf("[%v] ping <%v> <%v>\n", addr, nd.pre, nd.suc)
		if nd.Ping(nd.pre) && nd.Ping(nd.suc) {
			client, err := dial(nd.pre)
			if err != nil {
				panic(err)
				return
			}
			var res bool
			client.Call("Node.Set_suc", nd.suc, &res)
			client.Close()

			client2, err2 := dial(nd.suc)
			if err2 != nil {
				panic(err2)
				return
			}

			client2.Call("Node.Set_pre", nd.pre, &res)

			if nd.suc != nd.get_address() {
				for k, v := range nd.data {
					da := Pair{k, v}
					client2.Call("Node.Put_rpc", da, &res)
					delete(nd.data, k)
				}
			}

			client2.Close()
			nd.myServer.listen = false
			nd.exit <- true
			nd.myServer.listener.Close()
			return
		}
		fmt.Printf("[%v] ping failed\n", addr)
		time.Sleep(_ti)
	}
	//fmt.Printf("[%v] completed\n", nd.get_address())
}

func (nd *Node) ForceQuit() {
	if !nd.myServer.listen {
		return
	}
	nd.myServer.listen = false
	nd.exit <- true
	nd.myServer.listener.Close()
}

func get_ip() string {
	return GetLocalAddress()
}

func get_hash(str string) *big.Int {
	t := sha1.New()
	t.Write([]byte(str))
	res := t.Sum(nil)
	s := big.NewInt(1)
	s.SetBytes(res)
	return s
}

var mod = new(big.Int).Exp(big.NewInt(2), big.NewInt(sha1.Size*8), nil)

func calc(st *big.Int, sk int) *big.Int {
	add := big.NewInt(2)
	exp := big.NewInt(int64(sk) - 1)
	add.Exp(add, exp, nil)
	st.Add(st, add)
	return st.Mod(st, mod)
}
