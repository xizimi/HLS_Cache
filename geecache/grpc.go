package geecache

import (
	"context"
	"fmt"
	"geecache/consistenthash"
	"geecache/registry"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	pb "geecache/geecachepb"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	defaultReplicas = 50
)

// server 模块为geecache之间提供通信能力
// 这样部署在其他机器上的cache可以通过访问server获取缓存
// 至于找哪台主机 那是一致性哈希的工作了

var (
	//这个变量通常用于创建etcd客户端的配置，当你不需要定制化的配置时，可以直接使用 defaultEtcdConfig 这个预定义的配置。
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"}, // etcd服务器的地址，这里使用本地地址和默认端口
		DialTimeout: 5 * time.Second,                   // 连接超时时间
	}
)

// Server 和 Group 是解耦合的 所以server要自己实现并发控制
type Server struct {
	pb.UnimplementedGroupCacheServer                     //gRPC 自动生成的代码，用于实现 gRPC 的服务端接口。
	self                             string              // 当前服务器的地址，format: ip:port
	status                           bool                // 当前服务器的运行状态，true: running false: stop
	stopSignal                       chan error          // 用于接收通知，通知服务器停止运行。通常是其他组件发出的信号，例如 registry 服务，用于通知当前服务停止运行。
	mu                               sync.Mutex          //保护共享资源的互斥锁
	peers                            *consistenthash.Map //一致性哈希（consistent hash）映射，用于确定缓存数据在集群中的分布。
	clients                          map[string]*Client  //用于存储其他节点的客户端连接。键是其他节点的地址，值是与该节点建立的客户端连接
}

// NewServer 创建cache的 Server
func NewServer(self string) (*Server, error) {
	return &Server{
		self:    self,
		peers:   consistenthash.New(defaultReplicas, nil),
		clients: make(map[string]*Client),
	}, nil
}

// Get 实现了 Server 结构体用于处理 gRPC 客户端的请求
func (s *Server) Get(ctx context.Context, in *pb.Request) (*pb.Response, error) {
	group, key := in.GetGroup(), in.GetKey()
	resp := &pb.Response{}
	log.Printf("[Geecache_svr %s] Recv RPC request %s/%s", s.self, group, key)
	if key == "" {
		return resp, fmt.Errorf("key is required")
	}
	g := GetGroup(group)
	if g == nil {
		return resp, fmt.Errorf("group not found")
	}
	view, err := g.Get(key)
	if err != nil {
		return resp, err
	}
	// 将获取到的缓存数据序列化为 protobuf 格式，并存储在响应对象的 Value 字段中
	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		return resp, err
	}
	resp.Value = body
	return resp, nil
}

// Start 方法负责启动缓存服务，监听指定端口，注册 gRPC 服务至服务器
func (s *Server) Start() error {
	// ----------------- 阶段 1：自动发现 & 构建哈希环 -----------------
	// 启动时先尝试拉取一次，尽量让第一眼看到的人多一点
	// log.Println("[Discovery] Start to discover peers from etcd...")
	cli, err := clientv3.New(defaultEtcdConfig)
	if err != nil {
		log.Printf("[Discovery] Failed to connect to etcd: %v", err)
	} else {
		// 1. 从 etcd 拉取所有 geecache 开头的 key
		resp, err := cli.Get(context.Background(), "geecache/", clientv3.WithPrefix())
		cli.Close() // 拉完赶紧关闭
		if err == nil {
			var peers []string
			for _, kv := range resp.Kvs {
				addr := strings.TrimPrefix(string(kv.Key), "geecache/")
				peers = append(peers, addr)
			}

			// 2. 【关键】手动把自己加进去（防止此时 etcd 里还没自己）
			peers = append(peers, s.self)

			// 3. 把完整的列表喂给一致性哈希，建立连接
			if len(peers) > 0 {
				s.Set(peers...)
				// log.Printf("[Discovery] Success! Peers loaded: %v\n", peers)
			}
		} else {
			log.Printf("[Discovery] Failed to get keys from etcd: %v", err)
		}
	}
	// -----------------------------------------------------------------

	s.mu.Lock()
	if s.status == true {
		s.mu.Unlock()
		return fmt.Errorf("server already started")
	}
	// -----------------启动服务----------------------
	// 1. 设置status为true 表示服务器已在运行
	// 2. 初始化stop channel, 这用于通知registry stop keep alive
	// 3. 初始化tcp socket并开始监听
	// 4. 注册rpc服务至grpc 这样grpc收到request可以分发给server处理
	// ----------------------------------------------
	s.status = true
	s.stopSignal = make(chan error)

	port := strings.Split(s.self, ":")[1]
	lis, err := net.Listen("tcp", ":"+port) //监听指定的 TCP 端口，用于接受客户端的 gRPC 请求
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGroupCacheServer(grpcServer, s)

	// 启动注册协程：这一步是为了让 OTHER 节点以后能发现我
	go func() {
		err := registry.Register("geecache", s.self, s.stopSignal)
		if err != nil {
			log.Fatalf(err.Error())
		}
		// Close channel
		close(s.stopSignal)
		// Close tcp listen
		err = lis.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
		// log.Printf("[%s] Revoke service and close tcp socket ok.", s.self)
	}()

	go func() {
		ticker := time.NewTicker(5 * time.Second) // 每 10 秒同步一次集群视图
		defer ticker.Stop()
		for {
			select {
			case <-s.stopSignal: // 如果收到停止信号，退出循环
				log.Println("[UpdatePeers] Stop signal received, exiting update routine.")
				return
			case <-ticker.C: // 时间到了，开始干活
				// log.Println("[UpdatePeers] Syncing peers from etcd...")

				// 1. 连接 etcd
				updateCli, err := clientv3.New(defaultEtcdConfig)
				if err != nil {
					log.Printf("[UpdatePeers] Connect etcd failed: %v", err)
					continue
				}
				defer updateCli.Close()
				// 2. 拉取最新列表
				resp, err := updateCli.Get(context.Background(), "geecache/", clientv3.WithPrefix())
				if err != nil {
					log.Printf("[UpdatePeers] Get keys failed: %v", err)
					continue
				}
				var peers []string
				for _, kv := range resp.Kvs {
					addr := strings.TrimPrefix(string(kv.Key), "geecache/")
					peers = append(peers, addr)
				}
				// 3. 检查自己是否在列表里，双重保险（防止 etcd 里自己掉线了）
				hasSelf := false
				for _, p := range peers {
					if p == s.self {
						hasSelf = true
						break
					}
				}
				if !hasSelf {
					peers = append(peers, s.self)
				}
				// 4. 更新哈希环 
				if len(peers) > 0 {
					s.Set(peers...)
					// log.Printf("[UpdatePeers] Updated peer list: %v (count: %d)", peers, len(peers))
				}
			}
		}
	}()
	s.mu.Unlock()
	//启动 gRPC 服务器。grpcServer.Serve(lis) 会阻塞，处理客户端的 gRPC 请求，直到服务器关闭或发生错误。
	//如果服务器状态为运行状态（s.status 为 true），并且发生了错误，则返回相应的错误。
	if err := grpcServer.Serve(lis); s.status && err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}
	return nil
}

func (s *Server) Set(peers ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peers = consistenthash.New(defaultReplicas, nil)
	s.peers.Add(peers...)
	s.clients = make(map[string]*Client, len(peers))
	for _, peer := range peers {
		// service := fmt.Sprintf("geecache/%s", peer)
		// service := "geecache"
		s.clients[peer] = NewClient(peer)
	}
}

// PickPeer 方法，用于根据给定的键选择相应的对等节点
func (s *Server) PickPeer(key string) (PeerGetter, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lastSlash := strings.LastIndex(key, "/")
	peerAddr := s.peers.Get(key[:lastSlash]) //根据给定的键 key 选择相应的对等节点的地址 peerAddr
	if peerAddr == s.self {      //如果选择的节点地址与当前服务器的地址相同，说明该节点就是当前服务器本身
		// log.Printf("ooh! pick myself, I am %s\n", s.self)
		return nil, false
	}
	log.Printf("[cache %s] pick remote peer: %s\n", s.self, peerAddr)
	return s.clients[peerAddr], true //如果选择的节点不是当前服务器本身，日志会记录当前服务器选择了远程对等节点，并且函数会返回选择的对等节点的客户端连接（s.clients[peerAddr]）和 true，表示选择成功
}

// Stop 停止server运行 如果server没有运行 这将是一个no-op
func (s *Server) Stop() {
	s.mu.Lock()
	if s.status == false {
		s.mu.Unlock()
		return
	}
	s.stopSignal <- nil // 发送停止keepalive信号
	s.status = false    // 设置server运行状态为stop
	s.clients = nil     // 清空一致性哈希信息 有助于垃圾回收
	s.peers = nil       // 清空一致性哈希映射
	s.mu.Unlock()
}

var _ PeerPicker = (*Server)(nil)

type Client struct {
	baseURL string
}


func (c *Client) Get(in *pb.Request, out *pb.Response) error {
	// 打印一下，确认我们要连谁
	log.Printf("[Step Direct] Dialing peer: %s\n", c.baseURL)

	// 直接 grpc.Dial 目标 IP，不走 etcd 服务发现
	conn, err := grpc.Dial(c.baseURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(), // 阻塞直到连上
	)
	if err != nil {
		log.Printf("[Step Direct ERROR] Failed to dial %s: %v\n", c.baseURL, err)
		return err
	}
	defer conn.Close()

	log.Printf("[Step Direct OK] Connected to %s, sending RPC...\n", c.baseURL)

	// 创建 gRPC 客户端存根
	grpcClient := pb.NewGroupCacheClient(conn)

	// 设置超时上下文（比如 10 秒）
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 发起 RPC 调用
	response, err := grpcClient.Get(ctx, in)
	if err != nil {
		log.Printf("[gRPC Error] RPC failed: %v\n", err)
		return fmt.Errorf("reading response body: %v", err)
	}

	// 解析返回的数据
	if err = proto.Unmarshal(response.GetValue(), out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}

	return nil
}


// NewClient 创建一个远程节点客户端
func NewClient(service string) *Client {
	return &Client{
		baseURL: service,
	}
}

var _ PeerGetter = (*Client)(nil)
