package ownership

import "hash/crc32"

func Shard(channelID string, n int) int {
	return int(crc32.ChecksumIEEE([]byte(channelID)) % uint32(n))
}
