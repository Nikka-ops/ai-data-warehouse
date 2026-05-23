-- 原子性地 get-or-set 特征（避免缓存击穿）
-- KEYS[1]: Redis key
-- ARGV[1]: TTL 秒数
-- ARGV[2]: 待写入的 JSON 字符串
-- 返回：已存在的值（不覆盖）或刚写入的值

local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local value = ARGV[2]

-- 先尝试读取已有值
local existing = redis.call('GET', key)
if existing then
    return existing  -- 已有值直接返回，不覆盖
end

-- 不存在则原子写入并设置过期时间
redis.call('SETEX', key, ttl, value)
return value
