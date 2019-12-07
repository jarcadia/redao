--Keys setKey, objKey, 
local v = redis.call('hincrby', KEYS[2], 'v', 1);
if (v == 1) then
	redis.call('sadd', KEYS[1], string.sub(KEYS[2], string.len(KEYS[1]) + 2));
end
