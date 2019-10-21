package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

// 存储节点信息的结构体
type Node struct {
	Id   int    `json:"id"`   // 节点 id , 通过随机数生成
	Addr string `json:"addr"` // 节点 ip 地址
	Port string `json:"port"` // 节点端口号
}

func (n *Node) String() string {
	var buf bytes.Buffer

	buf.WriteString("Node {\n")
	buf.WriteString("  Id: " + strconv.Itoa(n.Id) + "\n")
	buf.WriteString("  Addr: " + n.Addr + "\n")
	buf.WriteString("  Port: " + n.Port + "\n")
	buf.WriteString("}")

	return buf.String()
}

func NewNode(id int, addr, port string) *Node {
	return &Node{
		Id:   id,
		Addr: addr,
		Port: port,
	}
}

// 一个节点到集群的一个请求或者响应的标准格式的结构体
type AddToClusterMessage struct {
	Source  Node   `json:"source"`
	Dest    Node   `json:"dest"`
	Message string `json:"message"`
}

func (req AddToClusterMessage) String() string {
	var buf bytes.Buffer

	buf.WriteString("AddToClusterMessage {\n")
	buf.WriteString("  Source: " + req.Source.String() + "\n")
	buf.WriteString("  Dest: " + req.Dest.String() + "\n")
	buf.WriteString("  Message: " + req.Message + "\n")
	buf.WriteString("}")

	return buf.String()
}

func connectToCluster(src, dest *Node) bool {
	//连接到socket的相关细节信息
	connOut, err := net.DialTimeout("tcp", dest.Addr+":"+dest.Port, 10*time.Second)
	if err != nil {
		if _, ok := err.(net.Error); ok {
			fmt.Println("不能连接到集群", src.Id)
			return false
		}
	} else {
		fmt.Println("连接到集群")
		text := "Hi nody.. 请添加我到集群"
		requestMessage := NewAddToClusterMessage(src, dest, text)
		json.NewEncoder(connOut).Encode(&requestMessage)

		decoder := json.NewDecoder(connOut)
		var responseMessage AddToClusterMessage
		decoder.Decode(&responseMessage)
		fmt.Println("得到数据响应:\n" + responseMessage.String())
		return true
	}

	return false
}

func NewAddToClusterMessage(src, dest *Node, text string) AddToClusterMessage {
	return AddToClusterMessage{
		Source: Node{
			Id:   src.Id,
			Addr: src.Addr,
			Port: src.Port,
		},
		Dest: Node{
			Id:   dest.Id,
			Addr: dest.Addr,
			Port: dest.Port,
		},
		Message: text,
	}
}

//me节点连接其它节点成功或者自身成为主节点之后开始监听别的节点在未来可能对它自身的连接
func listenOnPort(node Node) {
	//监听即将到来的信息
	ln, _ := net.Listen("tcp", ":"+node.Port)
	//接受连接
	for {
		connIn, err := ln.Accept()
		if err != nil {
			if _, ok := err.(net.Error); ok {
				fmt.Println("Error received while listening.", node.Id)
			}
		} else {
			var requestMessage AddToClusterMessage
			json.NewDecoder(connIn).Decode(&requestMessage)
			fmt.Println("Got request:\n" + requestMessage.String())

			text := "已添加你到集群"
			responseMessage := NewAddToClusterMessage(&node, &requestMessage.Source, text)
			json.NewEncoder(connIn).Encode(&responseMessage)
			connIn.Close()
		}
	}
}

func main() {
	// 当第一个节点启动用这个命令来将第一个节点作为主节点
	makeMasterOnError := flag.Bool("makeMasterOnError", false, "make this node master if unable to connect to the cluster ip provided.")

	// 设置要连接的目的地ip地址
	clusterip := flag.String("clusterip", "127.0.0.1:8001", "ip address of any node to connnect.")

	// 设置要连接的目的地端口号
	myport := flag.String("myport", "8001", "ip address to run this node on. default is 8001.")

	// 解析
	flag.Parse()

	rand.Seed(time.Now().UTC().UnixNano()) //种子
	myid := rand.Intn(9999999)

	// 获取ip地址
	myIp, _ := net.InterfaceAddrs()

	// 创建nodeInfo结构体
	src := NewNode(myid, myIp[0].String(), *myport)
	dest := NewNode(-1, strings.Split(*clusterip, ":")[0], strings.Split(*clusterip, ":")[1])
	fmt.Println("我的节点信息：", src.String())
	//尝试连接到集群，在已连接的情况下向集群发送请求
	ableToConnect := connectToCluster(src, dest)

	//如果dest节点不存在，则me节点为主节点启动，否则直接退出系统
	if ableToConnect || (!ableToConnect && *makeMasterOnError) {
		if *makeMasterOnError {
			fmt.Println("将启动me节点为主节点")
		}
		listenOnPort(*src)
	} else {
		fmt.Println("正在退出系统，请设置me节点为主节点")
	}
}
