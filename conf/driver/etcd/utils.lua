local ffi = require('ffi')

-- Just helper for assert_*() functions.
local function prefixed_error(prefix, message, ...)
    if prefix == nil then
        error(message:format(...))
    end
    error(('%s: %s'):format(prefix, message:format(...)))
end

-- Merge two tables recursively.
--
-- Prefer `a` value when both `a` and `b` have the same key and
-- values are not both tables.
local function merge_deep(a, b)
    if type(a) ~= 'table' or type(b) ~= 'table' then
        error('merge_deep: excepted tables')
    end
    local res = {}
    for k, v in pairs(a) do
        if type(v) == 'table' and type(b[k]) == 'table' then
            res[k] = merge_deep(v, b[k])
        else
            res[k] = v
        end
    end
    for k, v in pairs(b) do
        res[k] = v
    end
    return res
end

local function assert_string(value, prefix)
    if type(value) == 'string' then
        return
    end
    prefixed_error(prefix, 'Expected string, got %s', type(value))
end

local function assert_boolean(value, prefix)
    if type(value) == 'boolean' then
        return
    end
    prefixed_error(prefix, 'Expected boolean, got %s', type(value))
end

-- Based on code from tarantool/checks.
local function assert_uint64(value, prefix)
    if type(value) == 'number' then
        if value < 0 then
            prefixed_error(prefix, 'uint64 value should be non-negative')
        end
        if value >= 2^53 then
            prefixed_error(prefix, 'uint64 value should be < 2^53')
        end
        if math.floor(value) ~= value then
            prefixed_error(prefix, 'uint64 value should be integral')
        end
        return
    end

    if type(value) == 'cdata' then
        if ffi.istype('int64_t', value) then
            if value < 0 then
                prefixed_error(prefix, 'uint64 value should be non-negative')
            end
        elseif ffi.istype('uint64_t', value) then
            return
        end
        prefixed_error(prefix, 'uint64 value should be of int64_t or uint64 ' ..
            'ctype, got %s', tostring(ffi.typeof(value)))
    end

    prefixed_error(prefix, 'uint64 value should be number or number64 ' ..
        '(cdata), got %s', type(value))
end

-- Based on code from tarantool/checks.
local function assert_int64(value, prefix)
    if type(value) == 'number' then
        if value <= -2^53 then
            prefixed_error(prefix, 'int64 value should be > -2^53')
        end
        if value >= 2^53 then
            prefixed_error(prefix, 'int64 value should be < 2^53')
        end
        if math.floor(value) ~= value then
            prefixed_error(prefix, 'int64 value should be integral')
        end
        return
    end

    if type(value) == 'cdata' then
        if ffi.istype('int64_t', value) then
            return
        elseif ffi.istype('uint64_t', value) then
            if value >= 2^63 then
                prefixed_error(prefix, 'int64 value should be < 2^63')
            end
            return
        end
        prefixed_error(prefix, 'int64 value should be of int64_t or uint64 ' ..
            'ctype, got %s', tostring(ffi.typeof(value)))
    end

    prefixed_error(prefix, 'int64 value should be number or number64 ' ..
        '(cdata), got %s', type(value))
end


return {
    merge_deep = merge_deep,
    assert_string = assert_string,
    assert_boolean = assert_boolean,
    assert_int64 = assert_int64,
    assert_uint64 = assert_uint64,
}
