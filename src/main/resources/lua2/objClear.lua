--Keys setKey, hashKey, changeChannelKey
--Args [fields ...]
local changed = false;
local cleared = {};
local update = ''
for i=1,#ARGV do
	local prev = redis.call('hget', KEYS[2], ARGV[i]);
	if (prev) then
		redis.call('hdel', KEYS[2], ARGV[i]);
		changed = true;
		table.insert(cleared, ARGV[i]);
		table.insert(cleared, prev);
		update = update .. ',"' .. ARGV[i] .. '":null'
	end
end
if (changed) then
	local id = string.sub(KEYS[2], string.len(KEYS[1]) + 2);
	local ver = redis.call('hincrby', KEYS[2], 'v', 1);
	table.insert(cleared, 1, tostring(ver));
	redis.call('publish', KEYS[3], '{"' .. id .. '":{"v":'.. ver .. update .. '}}');
end
return cleared;

