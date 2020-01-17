--Keys setKey, objKey, changeChannelKey
local v = redis.call('hincrby', KEYS[2], 'v', 1);
if (v == 1) then
	local id = string.sub(KEYS[2], string.len(KEYS[1]) + 2);
	redis.call('sadd', KEYS[1], string.sub(KEYS[2], string.len(KEYS[1]) + 2));
	redis.call('publish', KEYS[3], '{"' .. id .. '":{"v":' .. v .. '}}');
end
return v;