--Keys setKey, hashKey, changeChannelKey
--Args field value [field value...] 
local changed = false;
local update = {};
local changes = {};
for i=1,#ARGV,2 do
	local prev = redis.call('hget', KEYS[2], ARGV[i]);
	if (prev ~= ARGV[i+1]) then
		redis.call('hset', KEYS[2], ARGV[i], ARGV[i+1]);
		changed = true;
		table.insert(changes, ARGV[i]);
		table.insert(changes, prev);
		table.insert(changes, ARGV[i+1]);
		update[ARGV[i]] = cjson.decode(ARGV[i+1])
	end
end
if (changed) then
	local id = string.sub(KEYS[2], string.len(KEYS[1]) + 2);
	local ver = redis.call('hincrby', KEYS[2], 'v', 1);
	update['v'] = ver;
	table.insert(changes, 1, tostring(ver));
	if (ver == 1) then
		redis.call('sadd', KEYS[1], id);
	end
	redis.call('publish', KEYS[3], '{"' .. id .. '":' .. cjson.encode(update) .. '}');
end
return changes;

