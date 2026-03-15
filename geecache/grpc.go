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
)

const (
	defaultReplicas = 50
	// 新增：Invalidate RPC 方法全路径
	invalidateMethod = "/geecachepb.GroupCache/Invalidate"
)

// server 模块为geecache之间提供通信能力
// 这样部署在其他机器上的cache可以通过访问server获取缓存
// 至于找哪台主机 那是一致性哈希的工作了
// type PeerHealth struct {
//     Healthy     bool
//     LastSeen    time.Time // 最后一次成功通信时间
//     FailCount   int       // 连续失败次数
// }
var (
	//这个变量通常用于创建etcd客户端的配置，当你不需要定制化的配置时，可以直接使用 defaultEtcdConfig 这个预定义的配置。
	defaultEtcdConfig = clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"}, // etcd服务器的地址，这里使用本地地址和默认端口
		DialTimeout: 5 * time.Second,                   // 连接超时时间
	}
)

// Server 和 Group 是解耦合的 所以 server 要自己实现并发控制
type Server struct {
	pb.UnimplementedGroupCacheServer                     //gRPC 自动生成的代码，用于实现 gRPC 的服务端接口。
	self                             string              // 当前服务器的地址，format: ip:port
	status                           bool                // 当前服务器的运行状态，true: running false: stop
	stopSignal                       chan error          // 用于接收通知，通知服务器停止运行。通常是其他组件发出的信号，例如 registry 服务，用于通知当前服务停止运行。
	mu                               sync.Mutex          //保护共享资源的互斥锁
	peers                            *consistenthash.Map //一致性哈希（consistent hash）映射，用于确定缓存数据在集群中的分布。
	clients                          map[string]*Client  //用于存储其他节点的客户端连接。键是其他节点的地址，值是与该节点建立的客户端连接
	// 新增：etcd 客户端和 watch 相关字段
	etcdClient                       *clientv3.Client
	watchChan                        clientv3.WatchChan
	// health map[string]*PeerHealth
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
	// 修复：直接返回数据，不要再次序列化
	resp.Value = view.ByteSlice()
	return resp, nil
}

// 新增：Invalidate 实现缓存失效接口
func (s *Server) Invalidate(ctx context.Context, in *pb.Request) (*pb.Response, error) {
	group, key := in.GetGroup(), in.GetKey()
	log.Printf("[Geecache_svr %s] Recv Invalidate request %s/%s", s.self, group, key)
	
	g := GetGroup(group)
	if g == nil {
		return &pb.Response{}, fmt.Errorf("group not found")
	}
	
	// 删除本地缓存（不删除 Redis，Redis 由 owner 节点统一删除）
	g.DeleteLocalCache(key)
	
	log.Printf("[Invalidate] 节点 %s 已删除本地缓存 %s（Redis 由 owner 节点删除）", s.self, key)
	
	return &pb.Response{}, nil
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
		// 保存 etcd 客户端供后续 watch 使用
		s.etcdClient = cli
		
		// 1. 从 etcd 拉取所有 geecache 开头的 key
		resp, err := cli.Get(context.Background(), "geecache/", clientv3.WithPrefix())
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
	// 1. 设置 status 为 true 表示服务器已在运行
	// 2. 初始化 stop channel, 这用于通知 registry stop keep alive
	// 3. 初始化 tcp socket 并开始监听
	// 4. 注册 rpc 服务至 grpc 这样 grpc 收到 request 可以分发给 server 处理
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
			log.Fatalf("%v", err)
		}
		// Close channel
		close(s.stopSignal)
		// Close tcp listen
		err = lis.Close()
		if err != nil {
			log.Fatalf("%v", err)
		}
		// log.Printf("[%s] Revoke service and close tcp socket ok.", s.self)
	}()

	// 新增：使用 etcd Watch 机制监听节点变化
	go func() {
		if s.etcdClient == nil {
			log.Println("[Watch] etcd client not available, skip watch mechanism")
			return
		}

		log.Println("[Watch] Start watching etcd for peer changes...")
		// 监听 geecache/ 前缀的所有键变化
		s.watchChan = s.etcdClient.Watch(context.Background(), "geecache/", clientv3.WithPrefix())

		for {
			select {
			case <-s.stopSignal: // 如果收到停止信号，退出循环
				log.Println("[Watch] Stop signal received, exiting watch routine.")
				return
			case watchResp, ok := <-s.watchChan:
				if !ok {
					log.Println("[Watch] Watch channel closed, attempting to reconnect...")
					// watch 连接断开，尝试重新建立连接
					time.Sleep(2 * time.Second)
					if s.etcdClient != nil {
						s.watchChan = s.etcdClient.Watch(context.Background(), "geecache/", clientv3.WithPrefix())
					}
					continue
				}

				// 检测到变化，记录变化类型
				for _, ev := range watchResp.Events {
					log.Printf("[Watch] Key %s %s", string(ev.Kv.Key), ev.Type)
				}

				// 重新拉取最新节点列表
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, err := s.etcdClient.Get(ctx, "geecache/", clientv3.WithPrefix())
				cancel()

				if err != nil {
					log.Printf("[Watch] Failed to get peers from etcd: %v", err)
					continue
				}

				var peers []string
				for _, kv := range resp.Kvs {
					addr := strings.TrimPrefix(string(kv.Key), "geecache/")
					peers = append(peers, addr)
				}

				// 检查自己是否在列表里，双重保险
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

				// 更新哈希环
				if len(peers) > 0 {
					s.Set(peers...)
					log.Printf("[Watch] Updated peer list: %v (count: %d)", peers, len(peers))
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
	// for _, peer := range peers {
	// 	// service := fmt.Sprintf("geecache/%s", peer)
	// 	// service := "geecache"
	// 	s.clients[peer] = NewClient(peer)
	// }
	for _, peer := range peers {
        if peer == s.self {
            continue
        }
        if _, exists := s.clients[peer]; !exists {
            s.clients[peer] = NewClient(peer)
        }
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

// Stop 停止 server 运行 如果 server 没有运行 这将是一个 no-op
func (s *Server) Stop() {
	s.mu.Lock()
	if s.status == false {
		s.mu.Unlock()
		return
	}
	s.stopSignal <- nil // 发送停止 keepalive 信号
	s.status = false    // 设置 server 运行状态为 stop
	s.clients = nil     // 清空一致性哈希信息 有助于垃圾回收
	s.peers = nil       // 清空一致性哈希映射
	
	// 新增：关闭 etcd 客户端连接
	if s.etcdClient != nil {
		s.etcdClient.Close()
		log.Println("[Stop] etcd client closed")
	}
	
	s.mu.Unlock()
}

var _ PeerPicker = (*Server)(nil)

type Client struct {
	baseURL string
	// 新增：连接池，复用 gRPC 连接
	connPool sync.Map // map[string]*grpc.ClientConn
}

// 新增：获取或创建连接
func (c *Client) getConn() (*grpc.ClientConn, error) {
	if conn, ok := c.connPool.Load(c.baseURL); ok {
		return conn.(*grpc.ClientConn), nil
	}
	
	conn, err := grpc.Dial(c.baseURL,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, err
	}
	
	c.connPool.Store(c.baseURL, conn)
	return conn, nil
}

// 新增：关闭所有连接
func (c *Client) Close() {
	c.connPool.Range(func(key, value interface{}) bool {
		if conn, ok := value.(*grpc.ClientConn); ok {
			conn.Close()
		}
		c.connPool.Delete(key)
		return true
	})
}

func (c *Client) Get(in *pb.Request, out *pb.Response) error {
	log.Printf("[Step Direct] Dialing peer: %s\n", c.baseURL)

	conn, err := c.getConn()
	if err != nil {
		log.Printf("[Step Direct ERROR] Failed to dial %s: %v\n", c.baseURL, err)
		return err
	}

	log.Printf("[Step Direct OK] Connected to %s, sending RPC...\n", c.baseURL)

	grpcClient := pb.NewGroupCacheClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := grpcClient.Get(ctx, in)
	if err != nil {
		log.Printf("[gRPC Error] RPC failed: %v\n", err)
		return fmt.Errorf("reading response body: %v", err)
	}

	out.Value = response.GetValue()

	return nil
}

// 新增：Invalidate 方法 - 调用远程节点的缓存失效接口
func (c *Client) Invalidate(key string, group string) error {
	conn, err := c.getConn()
	if err != nil {
		log.Printf("[Invalidate ERROR] Failed to dial %s: %v\n", c.baseURL, err)
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := &pb.Request{
		Group: group,
		Key:   key,
	}

	resp := &pb.Response{}

	// 修复：使用 grpc.Invoke 直接调用 RPC，不依赖生成的客户端方法
	err = conn.Invoke(ctx, invalidateMethod, req, resp)
	if err != nil {
		log.Printf("[Invalidate ERROR] RPC failed for key %s: %v\n", key, err)
		return fmt.Errorf("invalidating cache: %v", err)
	}

	log.Printf("[Invalidate OK] Successfully invalidated key %s on %s", key, c.baseURL)
	return nil
}

// NewClient 创建一个远程节点客户端
func NewClient(service string) *Client {
	return &Client{
		baseURL: service,
	}
}

var _ PeerGetter = (*Client)(nil)
