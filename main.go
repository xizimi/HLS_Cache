package main

import (
	"flag"
	"fmt"
	"geecache"
	"log"
	"os"
	"net/http"
	"strings"
	"strconv"
)

// var db = map[string]string{
// 	"Tom":  "630",
// 	"Jack": "589",
// 	"Sam":  "567",
// }

func createGroup() *geecache.Group {
	return geecache.NewGroup("scores", 2<<30, geecache.GetterFunc(
		func(key string) ([]byte, error) {
			// log.Printf("[LocalFS] loading file: %s", key)
			data, err := os.ReadFile(key)
			if err != nil {
				if os.IsNotExist(err) {
					return nil, fmt.Errorf("file not found: %s", key)
				}
				return nil, err
			}
			return data, nil
		}))
}

func startCacheServer(addr string, addrs []string, gee *geecache.Group) {
	peers := geecache.NewHTTPPool(addr)
	peers.Set(addrs...)
	gee.RegisterPeers(peers)
	log.Println("geecache is running at", addr)
	log.Fatal(http.ListenAndServe(addr[7:], peers))
}
func startAPIServer(apiAddr string, gee *geecache.Group) {
	http.Handle("/", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Path[1:] 
			if key == "" || key == "favicon.ico" {
				return
			}
			view, err := gee.Get(key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}

			data := view.ByteSlice()
			size := int64(len(data))
			w.Header().Set("Accept-Ranges", "bytes")
			rangeHeader := r.Header.Get("Range")
			if rangeHeader == "" {
				w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
				setContentType(w, key)
				w.WriteHeader(http.StatusOK)
				w.Write(data)
				return
			}
			if !strings.HasPrefix(rangeHeader, "bytes=") {
				http.Error(w, "Invalid Range unit", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			
			byteRange := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
			if len(byteRange) != 2 {
				http.Error(w, "Invalid Range format", http.StatusRequestedRangeNotSatisfiable)
				return
			}

			start, err := strconv.ParseInt(byteRange[0], 10, 64)
			if err != nil {
				http.Error(w, "Invalid Range start", http.StatusRequestedRangeNotSatisfiable)
				return
			}

			var end int64
			if byteRange[1] == "" {
				end = size - 1 
			} else {
				end, err = strconv.ParseInt(byteRange[1], 10, 64)
				if err != nil {
					http.Error(w, "Invalid Range end", http.StatusRequestedRangeNotSatisfiable)
					return
				}
			}

			if start >= size || end >= size || start > end {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
				http.Error(w, "Range Not Satisfiable", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			start=0
			chunkData := data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
			w.Header().Set("Content-Length", strconv.FormatInt(int64(len(chunkData)), 10))
			setContentType(w, key)
			w.WriteHeader(http.StatusPartialContent)
			w.Write(chunkData)
		}))

	log.Println("Frontend server is running at", apiAddr)
	log.Fatal(http.ListenAndServe(apiAddr[7:], nil))
}

// 辅助函数：设置 Content-Type
func setContentType(w http.ResponseWriter, key string) {
	if strings.HasSuffix(key, ".m3u8") {
		w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	} else if strings.HasSuffix(key, ".ts") {
		w.Header().Set("Content-Type", "video/mp2t")
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
}


// startCacheServerGrpcEtcd 函数：
// 创建一个 geecache.Server 实例，该实例用于处理 gRPC 请求并与其他节点通信。
// 通过 geecache.Server 实例的 Set 方法设置一组节点地址。
// 将 geecache.Server 实例注册到缓存组（gee）中。
// 启动 geecache.Server 实例，开始处理 gRPC 请求。
func startCacheServerGrpcEtcd(addr string, addrs []string, gee *geecache.Group) {
	peers, _ := geecache.NewServer(addr)
	peers.Set(addrs...)
	gee.RegisterPeers(peers)
	log.Println("geecache is running at ", addr)
	err := peers.Start()
	if err != nil {
		peers.Stop()
	}
}

/*
用户通过 API 服务器（例如 http://localhost:9999）访问 /api?key=XXX 的形式来获取缓存数据。
API 服务器会调用对应缓存组的 Get 方法。
Get 方法首先尝试从本地缓存中的热点缓存（hotCache）中查找数据。
如果数据不在热点缓存中，它将尝试从主缓存（mainCache）中查找数据。
如果主缓存中也没有数据，缓存系统将选择一个远程节点（可能是本地节点，也可能是其他节点）。
如果选中的远程节点是本地节点，缓存系统会直接从数据源获取数据。
如果选中的远程节点不是本地节点，API 服务器将发送 gRPC 请求给对应的远程节点，要求其提供数据。

注意：节点刚开始都会注册到etcd。
*/
func main() {
	var port int
	var api bool
	flag.IntVar(&port, "port", 8001, "GeeCache server port")
	flag.BoolVar(&api, "api", false, "Start a api server?")
	flag.Parse()

	apiAddr := "http://localhost:9988"
	addrMap := map[int]string{
		8004: "127.0.0.1:8004",
		8002: "127.0.0.1:8002",
		8003: "127.0.0.1:8003",
	} //grpc版本（含etcd）
	var addrs []string
	for _, v := range addrMap {
		addrs = append(addrs, v)
	}
	gee := createGroup()
	gee.StartPrewarmWorker()
	if api {
		go startAPIServer(apiAddr, gee)
	}
	startCacheServerGrpcEtcd(addrMap[port], addrs, gee) //grpc版本
}
