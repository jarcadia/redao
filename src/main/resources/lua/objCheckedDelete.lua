--Keys setKey, hashKey, changeChannelKey
--Args id
local removed = redis.call('del', KEYS[2]);
if (removed == 1) then 
	redis.call('srem', KEYS[1], ARGV[1]);
	redis.call('publish', KEYS[3], '{"' .. ARGV[1] .. '":null}')
end
return removed;
