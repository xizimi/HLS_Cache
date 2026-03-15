package geecache

import (
	"fmt"
	pb "geecache/geecachepb"
	"geecache/singleflight"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
	"regexp"
	"strconv"
	"strings"
	"os"
	"github.com/redis/go-redis/v9"
	"context"
	"errors"
	"github.com/bits-and-blooms/bloom/v3"
)

const (
	defaultHotCacheRatio      = 8
	defaultMaxMinuteRemoteQPS = 3
	SegmentPrefix = "segment_" 
	// 布隆过滤器配置
	bloomExpectedItems = 100000  // 预期元素数量
	bloomFalsePositiveRate = 0.01 // 误判率 1%
)

type VideoMeta struct {
	TotalSegments int // 总共有多少个切片
}
type VisitHistory struct {
	last2Seq [2]int // 记录最近两次访问的 segment 序号
}

// A Group is a cache namespace and associated data loaded spread over
type Group struct {
	mu sync.RWMutex
	name      string
	getter    Getter
	mainCache cache
	hotCache  cache
	peers     PeerPicker
	// use singleflight.Group tp make sure that
	// each key is only fetched once
	loader *singleflight.Group
	keys   map[string]*KeyStats
	prefetchHistory map[string]*VisitHistory 
	videoMetaMap map[string]*VideoMeta 
	lastDownloadTime time.Time
    // 修复：使用 Uint64+Float64bits 替代 Float64，兼容所有 Go 版本
    currentBandwidth atomic.Uint64
	prefetchSem chan struct{}
		// 新增：Redis 相关字段
	redisClient *redis.Client
    redisTTL    time.Duration
	// 新增：布隆过滤器
	bloomFilter *bloom.BloomFilter
    // 新增：写锁，防止并发写冲突
    writeMu sync.Map // map[string]*sync.Mutex
}

type KeyStats struct {
	firstGetTime time.Time
	remoteCnt    AtomicInt
}

type AtomicInt int64

func (i *AtomicInt) Add(n int64) {
	atomic.AddInt64((*int64)(i), n)
}

func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

// A Getter loads data for a key
type Getter interface {
	Get(key string) ([]byte, error)
	// 支持数据更新
	Set(key string, value []byte) error
	Delete(key string) error
}

// A GetterFunc implements Getter witha function
type GetterFunc func(key string) ([]byte, error)

// Get implements Getter inteface function
func (f GetterFunc) Get(key string) ([]byte, error) {
	return f(key)
}

// 新增：Set 实现（默认不支持，需要自定义 Getter）
func (f GetterFunc) Set(key string, value []byte) error {
	return fmt.Errorf("GetterFunc does not support Set operation")
}

// 新增：Delete 实现（默认不支持，需要自定义 Getter）
func (f GetterFunc) Delete(key string) error {
	return fmt.Errorf("GetterFunc does not support Delete operation")
}

var (
	groupsMu     sync.RWMutex
	groups = make(map[string]*Group)
)
func (g *Group) isOwner(key string) bool {
	if g.peers == nil {
		return true // 单机模式，总是 owner
	}
	_, ok := g.peers.PickPeer(key)
	return !ok // PickPeer 返回 false 表示本节点负责
}

// NewGroup creates a new instance of Group
// func NewGroup(name string, cacheBytes int64, getter Getter) *Group {
// 	if getter == nil {
// 		panic("nil Getter")
// 	}
// 	const defaultTTL = 5 * time.Minute
// 	groupsMu.Lock()
// 	defer groupsMu.Unlock()
// 	g := &Group{
// 		name:      name,
// 		getter:    getter,
// 		mainCache: cache{cacheBytes: cacheBytes, ttl: defaultTTL},
//         hotCache:  cache{cacheBytes: cacheBytes / defaultHotCacheRatio, ttl: defaultTTL * 2}, 
// 		loader:    &singleflight.Group{},
// 		keys:      make(map[string]*KeyStats),
// 		prefetchHistory: make(map[string]*VisitHistory),
// 		videoMetaMap:    make(map[string]*VideoMeta), 
// 		prefetchSem: make(chan struct{}, 20), 
// 	}
// 	groups[name] = g
// 	return g
// }
func NewGroup(name string, cacheBytes int64, getter Getter, redisClient *redis.Client) *Group {
    if getter == nil {
        panic("nil Getter")
    }
    const defaultTTL = 5 * time.Minute
    groupsMu.Lock()
    defer groupsMu.Unlock()
    g := &Group{
        name:      name,
        getter:    getter,
        mainCache: cache{cacheBytes: cacheBytes, ttl: defaultTTL},
        hotCache:  cache{cacheBytes: cacheBytes / defaultHotCacheRatio, ttl: defaultTTL * 2},
        loader:    &singleflight.Group{},
        keys:      make(map[string]*KeyStats),
        prefetchHistory: make(map[string]*VisitHistory),
        videoMetaMap:    make(map[string]*VideoMeta),
        prefetchSem: make(chan struct{}, 20),
        redisClient: redisClient,
        redisTTL:    30 * time.Minute,
        // 初始化布隆过滤器
        bloomFilter: bloom.NewWithEstimates(bloomExpectedItems, bloomFalsePositiveRate),
    }
    groups[name] = g
    return g
}

// 新增函数：支持 Redis

// GetGroup returns the named group previously created with NewGroup, or
// nil if there's no such group
func GetGroup(name string) *Group {
	groupsMu.RLock()
	g := groups[name]
	groupsMu.RUnlock()
	return g
}

func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	startTime := time.Now()

	// 1. HotCache
	if v, ok := g.hotCache.get(key); ok {
		log.Println("[GeeCache] hot cache hit")
		if g.isOwner(key) { 
			go g.checkPrefetch(key)
		}
		return v, nil
	}

	// 2. MainCache
	if v, ok := g.mainCache.get(key); ok {
		log.Println("[GeeCache] main cache hit")
		go g.updateKeyStats(key, v)
		if g.isOwner(key) { 
			go g.checkPrefetch(key)
		}
		return v, nil
	}
	if g.redisClient != nil {
        ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
        defer cancel()
        
        if data, err := g.redisClient.Get(ctx, key).Bytes(); err == nil {
            log.Println("[GeeCache] redis cache hit")
            value := ByteView{b: cloneBytes(data)}
            // 回填到本地 MainCache，加速下次访问
            g.populateCache(key, value)
            return value, nil
        }
    }

	value, err := g.load(key)
	if err != nil {
		return ByteView{}, err
	}
	duration := time.Since(startTime).Seconds()
	sizeMB := float64(value.Len()) / 1024 / 1024
	// 修复：使用原子操作更新带宽统计（Uint64+Float64bits）
	g.currentBandwidth.Store(math.Float64bits(sizeMB / duration))
	log.Printf("[Network] Downloaded %s, Size: %.2fMB, Time: %.2fs, Speed: %.2fMB/s", key, sizeMB, duration, math.Float64frombits(g.currentBandwidth.Load()))

	if g.isOwner(key) { 
		go g.checkPrefetch(key)
	}
	return value, nil
}

// 新增：Set 方法 - 更新数据并保持缓存一致性
func (g *Group) Set(key string, value []byte) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}
	
	// 修复：获取该 key 的写锁，防止并发写冲突
	writeLock := &sync.Mutex{}
	if actual, loaded := g.writeMu.LoadOrStore(key, writeLock); loaded {
		writeLock = actual.(*sync.Mutex)
	}
	writeLock.Lock()
	
	// 修复：使用 defer 确保锁一定被释放
	defer func() {
		writeLock.Unlock()
		// 清理写锁（避免内存泄漏）
		g.writeMu.Delete(key)
	}()
	
	// 1. 更新数据源
	if err := g.getter.Set(key, value); err != nil {
		return fmt.Errorf("failed to update data source: %v", err)
	}
	
	// 2. 更新本地缓存
	byteValue := ByteView{b: cloneBytes(value)}
	g.populateCache(key, byteValue)
	
	// 3. 修复：只有热点数据才写入 Redis
	//    判断标准：key 是否已在 hotCache 中，或访问统计是否达到热点阈值
	isHot := g.isHotKey(key)
	if g.redisClient != nil && isHot {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		ttl := g.calcTTL(key) + 10*time.Minute
		
		// 最多重试 3 次
		var err error
		for i := 0; i < 3; i++ {
			err = g.redisClient.Set(ctx, key, value, ttl).Err()
			if err == nil {
				log.Printf("[Redis] 更新热点缓存成功 %s", key)
				break
			}
			log.Printf("[Redis] 更新热点缓存失败 %s (尝试 %d/3): %v", key, i+1, err)
			time.Sleep(100 * time.Millisecond)
		}
		if err != nil {
			log.Printf("[Redis] 更新热点缓存最终失败 %s: %v", key, err)
		}
	} else if g.redisClient != nil {
		log.Printf("[Redis] 跳过非热点数据 %s，不写入 Redis", key)
	}
	
	// 4. 广播缓存失效到集群其他节点
	go g.broadcastInvalidate(key)
	
	log.Printf("[Update] Key %s updated successfully", key)
	return nil
}

// 新增：Delete 方法 - 删除数据并保持缓存一致性
func (g *Group) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("key is required")
	}
	
	// 修复：只有 owner 节点才执行完整的删除流程
	if !g.isOwner(key) {
		// 非 owner 节点：转发删除请求到 owner 节点
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if client, ok := peer.(*Client); ok {
					return client.Invalidate(key, g.name)
				}
			}
		}
		// 无法转发，降级处理
		log.Printf("[Delete] Non-owner node %s cannot forward delete for %s", g.name, key)
		return nil
	}
	
	// 1. 删除数据源
	if err := g.getter.Delete(key); err != nil {
		return fmt.Errorf("failed to delete data source: %v", err)
	}
	
	// 2. 删除本地缓存
	g.DeleteLocalCache(key)
	
	// 3. 删除 Redis 缓存（只有 owner 节点执行）
	if g.redisClient != nil && g.isOwner(key) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if err := g.redisClient.Del(ctx, key).Err(); err != nil {
			log.Printf("[Redis] 删除缓存失败 %s: %v", key, err)
		} else {
			log.Printf("[Redis] 删除缓存成功 %s (owner node)", key)
		}
	}
	
	// 4. 广播缓存失效到集群其他节点
	go g.broadcastInvalidate(key)
	
	log.Printf("[Delete] Key %s deleted successfully", key)
	return nil
}

// 新增：DeleteLocalCache - 删除本地缓存（不广播）
func (g *Group) DeleteLocalCache(key string) {
	g.mainCache.delete(key)
	g.hotCache.delete(key)
	// 从布隆过滤器中移除（如果需要）
	// 注意：标准布隆过滤器不支持删除，这里仅记录日志
	log.Printf("[LocalCache] Key %s removed from local cache", key)
}

// 新增：UpdateLocalCache - 更新本地缓存（不广播）
func (g *Group) UpdateLocalCache(key string, value []byte) {
	byteValue := ByteView{b: cloneBytes(value)}
	g.populateCache(key, byteValue)
	log.Printf("[LocalCache] Key %s updated in local cache", key)
}

// 新增：broadcastInvalidate - 广播缓存失效到集群所有节点
func (g *Group) broadcastInvalidate(key string) {
	if g.peers == nil {
		// 单机模式，不需要广播
		return
	}
	
	// 获取所有节点地址，通知它们失效
	if server, ok := g.peers.(*Server); ok {
		// 如果是 Server 类型，可以获取所有 clients
		server.mu.Lock()
		clientsCopy := make(map[string]*Client)
		for addr, client := range server.clients {
			clientsCopy[addr] = client
		}
		server.mu.Unlock()
		
		// 遍历所有 peer，通知它们失效（排除自己）
		for addr, client := range clientsCopy {
			if client == nil || addr == server.self {
				continue
			}
			go func(a string, c *Client) {
				err := c.Invalidate(key, g.name)
				if err != nil {
					log.Printf("[Broadcast] Failed to invalidate %s on %s: %v", key, a, err)
				}
			}(addr, client)
		}
	}
}

func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	//通过阻塞防止缓存击穿
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			if peer, ok := g.peers.PickPeer(key); ok {
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		return g.getLocally(key)
	})

	if err == nil {
		return viewi.(ByteView), nil
	}
	return 
}
func (g *Group) quiet_load(key string) (value ByteView, err error) {
    viewi, err := g.loader.Do(key, func() (interface{}, error) {
        if g.peers != nil {
            if peer, ok := g.peers.PickPeer(key); ok {
                return g.quiet_getFromPeer(peer, key)  // 远程获取，不缓存
            }
        }
        // 本地获取
        bytes, err := g.getter.Get(key)
        if err != nil {
            return nil, err
        }
        return ByteView{b: cloneBytes(bytes)}, nil
    })

    if err != nil {
        return ByteView{}, err
    }
    
    val := viewi.(ByteView)
    
    if g.peers == nil {
        // 单机模式：总是缓存
        g.populateCache(key, val)
    } else {
        // 分布式模式：只有 PickPeer 返回 false（即本节点负责）才缓存
        if _, ok := g.peers.PickPeer(key); !ok {
            g.populateCache(key, val)
        }
    }
    return val, nil
}

func (g *Group) quiet_getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	// log.Println("[Get] go to get")
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}

	value := ByteView{b: res.Value}
	// log.Println("[Get] already get", err)

	return value, nil
}
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	// log.Println("[Get] go to get")
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}

	value := ByteView{b: cloneBytes(res.Value)}
	// log.Println("[Get] already get", err)
	g.updateKeyStats(key, value)

	return value, nil
}
func (g *Group) isM3U8(key string) bool {
    return strings.HasSuffix(key, ".m3u8") || strings.HasSuffix(key, ".mpd")
}
func (g *Group) updateKeyStats(key string, value ByteView) {
    var firstGetTime time.Time
    var currentCount int64
    var isNew bool

    // --- 阶段 1：仅对 keys map 的读写加锁 (极快) ---
    g.mu.Lock()
    if stat, ok := g.keys[key]; ok {
        stat.remoteCnt.Add(1)
        firstGetTime = stat.firstGetTime
        currentCount = stat.remoteCnt.Get()
    } else {
        // 首次访问
        g.keys[key] = &KeyStats{
            firstGetTime: time.Now(),
            remoteCnt:    1,
        }
        isNew = true
    }
    g.mu.Unlock()

    // 如果是首次访问，直接返回，还没资格晋升
    if isNew {
        return
    }

    // --- 阶段 2：计算逻辑和判断 (不加锁，避免死锁) ---
    // 这里调用 isTail，isTail 内部会调用 ensureVideoMeta -> 再次尝试获取 mu
    // 此时 mu 已经释放，所以不会死锁
    threshold := int64(defaultMaxMinuteRemoteQPS)
    if g.isHead(key) || g.isTail(key)|| g.isM3U8(key) {
        threshold = threshold / 2
    }

    interval := math.Max(1, float64(time.Now().Unix()-firstGetTime.Unix())/60)
    qps := currentCount / int64(math.Round(interval))

    // --- 阶段 3：如果满足条件，晋升并删除统计 ---
    if qps >= threshold {
        g.populateHotCache(key, value)
        g.mu.Lock() // 再次加锁仅仅为了删除 map 里的 key
        delete(g.keys, key)
        g.mu.Unlock()
        log.Printf("[Promotion] Key %s promoted to HotCache (QPS: %d)", key, qps)
    }
}


// func (g *Group) getLocally(key string) (ByteView, error) {
// 	bytes, err := g.getter.Get(key)
// 	if err != nil {
// 		return ByteView{}, err
// 	}
// 	value := ByteView{b: cloneBytes(bytes)}
// 	g.populateCache(key, value)
// 	return value, nil
// }
func (g *Group) getLocally(key string) (ByteView, error) {
	// 使用布隆过滤器检查 key 是否存在
	if g.bloomFilter != nil && !g.bloomFilter.Test([]byte(key)) {
		log.Printf("[BloomFilter] Key %s does not exist (cache penetration prevented)", key)
		return ByteView{}, errors.New("key not found (bloom filter)")
	}
	
	bytes, err := g.getter.Get(key)
	if err != nil {
		if os.IsNotExist(err) {
			// 布隆过滤器已拦截，这里理论上不会执行到
			// 但为了安全，仍然记录日志
			log.Printf("[BloomFilter] False positive for key %s", key)
			return ByteView{}, errors.New("key not found")
		}
		return ByteView{}, err
	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	// 成功加载后，将 key 添加到布隆过滤器
	if g.bloomFilter != nil {
		g.bloomFilter.Add([]byte(key))
	}
	return value, nil
}

func (g *Group) populateCache(key string, value ByteView) {
	ttl := g.calcTTL(key)
	g.mainCache.add(key, value, ttl)
}

func (g *Group) populateHotCache(key string, value ByteView) {
	ttl := g.calcTTL(key)
	g.hotCache.add(key, value, ttl)
	if g.redisClient != nil {
        go func() {
            ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
            defer cancel()
            redisTTL := ttl + 10*time.Minute 
            if err := g.redisClient.Set(ctx, key, value.ByteSlice(), redisTTL).Err(); err != nil {
                log.Printf("[Redis] 写入失败 %s: %v", key, err)
            } else {
                log.Printf("[Redis] 写入成功 %s", key)
            }
        }()
    }
	// 成功加载后，将 key 添加到布隆过滤器
	if g.bloomFilter != nil {
		g.bloomFilter.Add([]byte(key))
	}
}

// RegisterPeers registers a PeerPicker for choosing remote peer.
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}
// 辅助函数：判断是否为片头 (假设 segment_000 到 segment_009 为片头)
func (g *Group)isHead(key string) bool {
	matched, _ := regexp.MatchString(`segment_00\d\.ts$`, key)
	return matched
}

// 辅助函数：判断是否为片尾 (假设 segment_90 到 segment_99 为片尾)
func (g *Group) isTail(key string) bool {
	seq := g.getSeq(key)
	if seq == -1 {
		return false
	}

	// 提取 vidPrefix (例如 /data/movie1/1080p)
	lastSlash := strings.LastIndex(key, "/")
	if lastSlash == -1 {
		return false
	}
	vidPrefix := key[:lastSlash]
	// log.Printf("Vidprefix", vidPrefix)

	// 1. 确保元数据已加载
	g.ensureVideoMeta(vidPrefix)

	// 2. 查询元数据
	g.mu.RLock()
	meta, ok := g.videoMetaMap[vidPrefix]
	g.mu.RUnlock()

	// 如果解析失败（m3u8 不存在），默认不是片尾，防止误判
	if !ok {
		return false
	}

	// 3. 判定：最后 5 个 segment 算作片尾
	tailSize := 5
	return seq >= (meta.TotalSegments - tailSize) && seq <= meta.TotalSegments
}
func (g *Group) getSeq(key string) int {
	// 动态使用常量构建正则
	re := regexp.MustCompile(SegmentPrefix + `(\d+)\.ts`)
	matches := re.FindStringSubmatch(key)
	if len(matches) < 2 {
		return -1
	}
	seq, _ := strconv.Atoi(matches[1])
	return seq
}

// checkPrefetch 检查访问序列，触发智能预取
func (g *Group) checkPrefetch(currentKey string) {
	// 提取当前 segment 的序号
	seq := g.getSeq(currentKey)
	if seq == -1 {
		return // 不是 ts segment，不处理
	}

	//  提取视频 ID 
	lastSlash := strings.LastIndex(currentKey, "/")
	if lastSlash == -1 {
		return
	}
	vidPrefix := currentKey[:lastSlash]

	// 获取或初始化该视频的访问历史
	g.mu.Lock()
	hist, exists := g.prefetchHistory[vidPrefix]
	if !exists {
		hist = &VisitHistory{last2Seq: [2]int{-1, -1}}
		g.prefetchHistory[vidPrefix] = hist
	}
	
	// 更新历史：把当前 seq 推进去
	prev := hist.last2Seq[1]
	hist.last2Seq[0] = hist.last2Seq[1]
	hist.last2Seq[1] = seq
	g.mu.Unlock()
	if prev != -1 && prev+1 == seq {
        // 根据网速决定预取数量
        prefetchCount := 2  // 默认 2 个
        // 修复：使用原子操作读取带宽（Uint64+Float64bits）
        bandwidth := math.Float64frombits(g.currentBandwidth.Load())
        if bandwidth > 0 && bandwidth < 0.5 {
			return
		}
        if bandwidth > 10 {       
            prefetchCount = 5                // 激进预取 5 个
            log.Printf("[Adaptive] Fast network (%.2fMB/s), prefetch 5 segments", bandwidth)
        } else if bandwidth > 2 {   // 网速 2-10MB/s
            prefetchCount = 3                // 正常预取 3 个
            log.Printf("[Adaptive] Normal network (%.2fMB/s), prefetch 3 segments", bandwidth)
        } else if bandwidth > 0 {   // 网速 < 2MB/s
            prefetchCount = 1                // 保守预取 1 个
            log.Printf("[Adaptive] Slow network (%.2fMB/s), prefetch only 1 segment", bandwidth)
        } else {
            log.Printf("[Adaptive] Unknown bandwidth, use default prefetch 2")
        }
        
        // 原来的循环 2 改成变量 prefetchCount
        for i := 1; i <= prefetchCount; i++ {
            nextSeq := seq + i
            nextKey := fmt.Sprintf("%s/segment_%03d.ts", vidPrefix, nextSeq)
			if _, ok := g.mainCache.get(nextKey); ok {
                continue
            }
            if _, ok := g.hotCache.get(nextKey); ok {
                continue
            }
            select {
    		case g.prefetchSem <- struct{}{}:
            go func(k string) {
                // 如果网速慢，预取也放慢（避免抢占带宽）
					defer func() { <-g.prefetchSem }()
					// 修复：使用原子操作读取带宽（Uint64+Float64bits）
					if math.Float64frombits(g.currentBandwidth.Load()) < 1 && math.Float64frombits(g.currentBandwidth.Load()) > 0 {
						time.Sleep(500 * time.Millisecond)  // 慢网时延迟 500ms 再预取
					}
					// 修复：将 Get_pred 替换为 quiet_load
					_, _ = g.quiet_load(k)
				}(nextKey)
			default:
				log.Printf("[Prefetch] Semaphore full, skip %s", nextKey)
    		}
        }
    }
	if len(g.prefetchHistory) > 1000 {
        for k := range g.prefetchHistory {
            delete(g.prefetchHistory, k)
            break
        }
    }
}
// StartPrewarmWorker 启动后台预热 worker (需要在 main 函数中调用)
func (g *Group) StartPrewarmWorker() {
	go func() {
		ticker := time.NewTicker(40 * time.Second) 
		for range ticker.C {
			// log.Println("[Prewarm] Worker started...")
			g.runPrewarm()
		}
	}()
}

// runPrewarm 执行具体的预热逻辑
func (g *Group) runPrewarm() {
    // 策略：确保所有视频的片头（前 3 个）永远在缓存中（冷启动保护）
    g.mu.RLock()
    vidPrefixes := make([]string, 0, len(g.videoMetaMap))
    for vid := range g.videoMetaMap {
        vidPrefixes = append(vidPrefixes, vid)
    }
    g.mu.RUnlock()

    for _, vidPrefix := range vidPrefixes {
        for i := 0; i < 3; i++ {
            key := fmt.Sprintf("%s/segment_%03d.ts", vidPrefix, i)
            if !g.isOwner(key) {
				continue
			}
			if _, ok := g.mainCache.get(key); !ok {
				// 修复：将 Get_pred 替换为 quiet_load
				g.quiet_load(key)
}
        }
    }
}
func (g *Group) ensureVideoMeta(vidPrefix string) {
    // 1. 快速检查（加锁）
    g.mu.Lock()
    _, exists := g.videoMetaMap[vidPrefix]
    g.mu.Unlock()

    if exists {
        return
    }

    // 2. 构造 m3u8 的路径
    m3u8Key := fmt.Sprintf("%s/playlist.m3u8", vidPrefix)
    bytes, err := os.ReadFile(m3u8Key)
    
    if err != nil {
        log.Printf("[Meta] Local file not found: %s, skipping meta load", m3u8Key)
        return
    }

    // 4. 解析内容
    content := string(bytes)
    re := regexp.MustCompile(SegmentPrefix + `\d+\.ts`)
    matches := re.FindAllString(content, -1)
    totalCount := len(matches)

    // 5. 只有成功解析出数量，才写入 map
    if totalCount > 0 {
        g.mu.Lock()
        g.videoMetaMap[vidPrefix] = &VideoMeta{TotalSegments: totalCount}
        g.mu.Unlock()
        // log.Printf("[Meta] Loaded meta locally for %s, total: %d", vidPrefix, totalCount)
        
        // 预加载该视频的所有 segment key 到布隆过滤器
        for i := 0; i < totalCount; i++ {
            segmentKey := fmt.Sprintf("%s/segment_%03d.ts", vidPrefix, i)
            if g.bloomFilter != nil {
                g.bloomFilter.Add([]byte(segmentKey))
            }
        }
        // 也将 m3u8 文件加入布隆过滤器
        if g.bloomFilter != nil {
            g.bloomFilter.Add([]byte(m3u8Key))
        }
        log.Printf("[BloomFilter] Preloaded %d segments for %s", totalCount, vidPrefix)
    }
}

func (g *Group) calcTTL(key string) time.Duration {
	if g.isM3U8(key) {
		return 30 * time.Minute  // m3u8 索引更新快 分钟
	}
	if g.isHead(key) {
		return 30 * time.Minute // 片头长期保留（提升秒开率）
	}
	if g.isTail(key) {
		return 15 * time.Minute // 片尾保完整性，比中间长
	}
	return 5 * time.Minute
}

// 新增：isHotKey - 判断 key 是否为热点数据
func (g *Group) isHotKey(key string) bool {
	// 1. 检查是否在 hotCache 中
	if _, ok := g.hotCache.get(key); ok {
		return true
	}
	
	// 2. 检查访问统计是否达到热点阈值
	g.mu.RLock()
	stat, exists := g.keys[key]
	g.mu.RUnlock()
	
	if !exists {
		return false
	}
	
	// 计算 QPS
	firstGetTime := stat.firstGetTime
	currentCount := stat.remoteCnt.Get()
	threshold := int64(defaultMaxMinuteRemoteQPS)
	
	// 片头片尾阈值减半
	if g.isHead(key) || g.isTail(key) || g.isM3U8(key) {
		threshold = threshold / 2
	}
	
	interval := math.Max(1, float64(time.Now().Unix()-firstGetTime.Unix())/60)
	qps := currentCount / int64(math.Round(interval))
	
	return qps >= threshold
}
