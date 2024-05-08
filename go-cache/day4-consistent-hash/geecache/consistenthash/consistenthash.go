package consistenthash

import (
	"hash/crc32" // 导入哈希算法库
	"sort"       // 导入排序库
	"strconv"    // 导入字符串转换库
)

// Hash maps bytes to uint32
type Hash func(data []byte) uint32 // 定义哈希函数类型

// Map constains all hashed keys
type Map struct {
	hash     Hash           // 哈希函数
	replicas int            // 虚拟节点倍数
	keys     []int          // 排序的哈希环
	hashMap  map[int]string // 虚拟节点与真实节点的映射关系
}

// New creates a Map instance
func New(replicas int, fn Hash) *Map {
	m := &Map{ // 创建 Map 实例
		replicas: replicas,             // 设置虚拟节点倍数
		hash:     fn,                   // 设置哈希函数
		hashMap:  make(map[int]string), // 初始化虚拟节点与真实节点的映射关系
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE // 默认哈希函数为 CRC32
	}
	return m
}

// Add adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys { // 遍历传入的节点
		for i := 0; i < m.replicas; i++ { // 每个节点创建 m.replicas 个虚拟节点
			hash := int(m.hash([]byte(strconv.Itoa(i) + key))) // 计算哈希值
			m.keys = append(m.keys, hash)                      // 将哈希值添加到哈希环中
			m.hashMap[hash] = key                              // 将哈希值与真实节点的映射关系保存到哈希表中
		}
	}
	sort.Ints(m.keys) // 对哈希环进行排序
}

// Get gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return "" // 如果哈希环中没有节点，返回空字符串
	}

	hash := int(m.hash([]byte(key))) // 计算键的哈希值
	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool { // 二分查找找到最接近的节点
		return m.keys[i] >= hash
	})

	return m.hashMap[m.keys[idx%len(m.keys)]] // 返回最接近节点的标识
}
