local ffi = require('ffi')
local m = require('conf.math')

-- Data layout.
--
-- Formal rules:
--
-- 1. `null` means nil.
-- 2. `true` and `false` mean boolean values.
-- 3. `-?\d+` is an integer number. (`-?0[xX][0-9a-fA-F]+` is reserved
--    for integers in hex.)
-- 4. `-?\d+(\.\d+)?([eE][+-]?\d+)?`, `inf`, `-inf`, `nan`, `-nan`
--    are  floating point numbers.
--    (`-?0[xX][0-9a-fA-F]+(\.[0-9a-fA-F]+)?([pP][+-]?\d+)?` is
--    reserved for floating point numbers in hex.)
-- 5. `!!.*` is a tagged value.
--    - A well-formed tagged value have one whitespace between
--      a tag name and a value.
--    - The only defined tag is `!!str`. A future implementation
--      may add more tags.
-- 6. Any other value is a string.
-- 7. Any string is interpreted as is: no backslash or quotes
--    processing performed.
--
-- Important point: a floating point number either has a dot or
-- represented in the scientific notation. Just `-?\d+` is always
-- an integer.
--
-- TODO: Consider alternative rules (just raw idea about rules
-- simplification):
--
-- 1, 2 - same.
-- 3. A value that starts from minus (`-`), a digit (including
--    `0x...`, `0X...`) or equal to `inf`, `-inf`, `nan`, `-nan`
--    is integer or floating point number.
-- 4. `-?\d+`, `-?0[xX][0-9a-fA-F]+` are integers. Other numeric
--    values are floating point numbers.
-- 5-7 - same.
--
-- If we'll apply those alternative rules, strings like '0day'
-- will require the explicit tag: `!!str 0day`.
--
-- Data layout in examples:
--
-- | Lua                     | etcd                       |
-- | ----------------------- | -------------------------- |
-- | box.NULL / nil          | null                       |
-- | boolean (true)          | true                       |
-- | boolean (false)         | false                      |
-- | number (integer)        | -?\d+                      |
-- | number64 (integer)      | -?\d+                      |
-- | number (floating point) | -?\d+\.\d+                 |
-- | number (floating point) | -?\d+\.\d+[eE][+-]?\d+     |
-- | number (inf)            | inf                        |
-- | number (-inf)           | -inf                       |
-- | number (nan)            | nan (-nan is accepted too) |
-- | string ('foo')          | foo                        |
-- | string ('true')         | !!str true                 |
-- | string ('4.5')          | !!str 4.5                  |
-- | string ('nan')          | !!str nan                  |
-- | string ('!!foo')        | !!str !!foo                |
-- | reserved                | -?0[xX][0-9a-fA-F]+        |
-- | reserved                | -?0[xX][0-9a-fA-F]+(\.[0-9a-fA-F]+)?([pP][+-]?\d+)? |
--
-- `number64` means ctype<int64_t> and ctype<uint64_t>. Other
-- cdata types are not supported yet. A future implementation
-- may add more types, e.g. ctype<float>, ctype<double>.
--
-- This format aims to achieve the following goals:
--
-- * Represent all scalar Lua values (plus our own number64) in an
--   unambiguous and human readable format.
-- * Ease direct data access: don't bother an administrator with
--   extra quotes and so on.
-- * Keep the format simple: so any value is easy to interpret
--   visually.
--
-- Those points should answer questions 'Why not MsgPack / JSON /
-- YAML?'.
--
-- Reserving of the hex integer / floating point number aims two
-- goals: represent double values exactly (without precision loss)
-- in a future and eliminate the chance to confuse 0x... value
-- meaning (number / string).

-- Encoding / decoding rules.
--
-- Encode: from Lua value to the string representation.
-- Decode: vice versa.
--
-- * Encode nil and box.NULL as null.
-- * Encode a `string` without a tag when possible.
-- * Encode a `number` as an integer value if it has no fractional
--   part and fits into the [-10^14 + 1; 10^14 - 1] range,
--   otherwise encode as a floating point number[^1].
-- * Encoding of a floating point number may silently loss
--   precision (decimal precision 14 is used).
-- * Encoding of cdata and userdata does not call metamethods
--   (such as __tostring, __serialize). A future implementation
--   may add this ability under an option.
--
-- [^1]: This way we'll always marshal a `number` value back into
--       a `number`.
--
--       Note that it differs from a behaviour of the built-in
--       msgpack serializer: it encodes any `number` without a
--       fractional part as integer, so marshalling of a `number`
--       value may give `number64` as result.
--
-- * Decode null as box.NULL[^2].
-- * Decode an integer number into `number` if it is in the
--   [-10^14 + 1; 10^14 - 1] range, otherwise into `number64`[^3].
-- * Raise an error on attempt to decode an integer number out of
--   the [-2^63; 2^64-1] range.
-- * Raise an error on attempt to decode a value with an unknown
--   tag.
-- * Decoding of a floating point number may silently loss
--   precision.
--
-- [^2]: Resemble the behaviour of tarantool's built-in msgpack,
--       json, yaml decoders. Intuitively it seems logical to
--       decode it as `nil` instead. However there is the
--       motivating case that suggests the opposite. Say we
--       flattenned and stored a table like `{foo = box.NULL}`.
--       The `box.NULL` value is stored as `null`. We should
--       decode this `null` back to `box.NULL` to assemble a
--       table similar to the source one.
--
-- [^3]: Resemble the behaviour of the built-in decoders and
--       tonumber64(), see also [1].
--
-- [1]: https://github.com/tarantool/tarantool/issues/1279
--
-- The schema below illustrates how integer values are marshalled
-- from / to Lua.
--
--       +-------- <number64> is always encoded as integer ----------+
--       |                                                           |
--       |     +-- encode <number> as floating point number ----+    |
--       |     |                                                |    |
--       |     |   +---- encode <number> as integer ----+       |    |
--       |     |   |  (when it has no fractional part)  |       |    |
--       |   <-+   |                                    |       +->  |
-- -------------------------------------------------------------------------
-- ... -2^63 ... 10^14 + 1 .......................... 10^14 - 1 ... 2^64 ...
-- -------------------------------------------------------------------------
-- <-+   |         |                                    |            |   +->
--   |   |         +--- decode integer into <number> ---+            |   |
--   |   |                                                           |   |
--   |   +------------ decode integer into <number64> ---------------+   |
--   |                                                                   |
--   +----------------- error on decoding an integer --------------------+

-- {{{ Data format checkers

-- is_in_*_format() functions determine whether an encoded value
-- adheres particular format. Those functions do not verify
-- whether the value is correct.

local function is_in_null_format(str)
    assert(type(str) == 'string')
    return str == 'null'
end

local function is_in_boolean_format(str)
    assert(type(str) == 'string')
    return str == 'false' or str == 'true'
end

local function is_in_integer_format(str)
    assert(type(str) == 'string')
    return str:match('^%-?[0-9]+$')
end

local function is_in_integer_hex_format(str)
    assert(type(str) == 'string')
    return str:match('^%-?0[xX][0-9a-fA-F]+$')
end

local function is_in_float_format(str)
    assert(type(str) == 'string')
    return str == 'inf' or str == '-inf' or str == 'nan' or str == '-nan' or
        str:match('^%-?[0-9]+%.[0-9]+$') or
        str:match('^%-?[0-9]+[eE][+-]?[0-9]+$') or
        str:match('^%-?[0-9]+%.[0-9]+[eE][+-]?[0-9]+$')
end

local function is_in_float_hex_format(str)
    assert(type(str) == 'string')
    return str:match('^%-?0[xX][0-9a-fA-F]+%.[0-9a-fA-F]+$') or
        str:match('^%-?0[xX][0-9a-fA-F]+[pP][+-]?[0-9]+$') or
        str:match('^%-?0[xX][0-9a-fA-F]+%.[0-9a-fA-F]+[pP][+-]?[0-9]+$')
end

local function is_in_tagged_format(str)
    assert(type(str) == 'string')
    return str:startswith('!!')
end

-- }}} Data format checkers

-- {{{ Encoding functions

-- encode_*() functions transform a scalar Lua value into a
-- string representation.

-- All integers in the [-2^53; 2^53] range are encoded exactly
-- in the binary64 IEEE754 format. Some (but not all) integers
-- are encoded exactly outside of this range (say, all even
-- integers in the range [2^53; 2^54]).
--
-- However we shrink the range of values to encode as integers
-- down to [-10^14 + 1; 10^14 - 1] for symmetry with decoding,
-- which, in turn, leans on the tonumber64() behaviour (see
-- link [1] above).
--
-- This symmetry allows us to store a <number> Lua value and fetch
-- it as <number> back for all numbers. Under the hood those
-- numbers will be stored as integers or floats, see the encoding
-- rules above.
local encode_number_as_integer_lowest = -99999999999999 -- -10^14 + 1
local encode_number_as_integer_highest = 99999999999999 -- 10^1 - 1

local function encode_null(value)
    assert(value == nil)
    return 'null'
end

local function encode_boolean(value)
    assert(type(value) == 'boolean')
    return value and 'true' or 'false'
end

local function encode_number(value)
    assert(type(value) == 'number')
    if m.isnan(value) then
        return 'nan'
    elseif m.ispinf(value) then
        return 'inf'
    elseif m.isninf(value) then
        return '-inf'
    elseif m.isinteger(value) and
            value >= encode_number_as_integer_lowest and
            value <= encode_number_as_integer_highest then
        -- Encode into the integer format.
        local res = ('%d'):format(value)
        assert(is_in_integer_format(res))
        return res
    end

    -- Encode into the floating point format.
    assert(m.isfinite(value))
    local res = ('%.14g'):format(value)
    if is_in_integer_format(res) then
        res = res .. '.0'
    end
    assert(is_in_float_format(res))
    return res
end

local function encode_number64(value)
    assert(type(value) == 'cdata')
    local res = tostring(value):gsub('U?LL$', '')
    assert(is_in_integer_format(res))
    return res
end

local function encode_cdata(value)
    assert(type(value) == 'cdata')
    if ffi.istype('void *', value) and value == nil then
        return encode_null(value)
    elseif ffi.istype('uint64_t', value) then
        return encode_number64(value)
    elseif ffi.istype('int64_t', value) then
        return encode_number64(value)
    end
    error(('No encoder for %s'):format(tostring(ffi.typeof(value))))
end

local function encode_tagged_string(value)
    assert(type(value) == 'string')
    return ('!!str %s'):format(value)
end

local function encode_string(value)
    assert(type(value) == 'string')
    local encode_with_tag =
        is_in_null_format(value) or
        is_in_boolean_format(value) or
        is_in_integer_format(value) or
        is_in_integer_hex_format(value) or
        is_in_float_format(value) or
        is_in_float_hex_format(value) or
        is_in_tagged_format(value)
    if encode_with_tag then
        return encode_tagged_string(value)
    end
    return value
end

local encode_funcs = {
    ['nil'] = encode_null,
    ['boolean'] = encode_boolean,
    ['number'] = encode_number,
    ['cdata'] = encode_cdata,
    ['string'] = encode_string,
}

local function encode(value)
    local lua_type = type(value)
    local func = encode_funcs[lua_type]
    if func == nil then
        error(('No encoder for %s'):format(lua_type))
    end
    return func(value)
end

-- }}} Encoding functions

-- {{{ Decoding functions

-- decode_*() functions transform a string representation into
-- a Lua value.

local function decode_null(str)
    assert(str == 'null')
    return box.NULL
end

local function decode_boolean(str)
    if str == 'false' then
        return false
    elseif str == 'true' then
        return true
    end
    error(('Unable to decode "%s" as boolean'):format(str))
end

local function decode_integer(str)
    local res = tonumber64(str)
    if res == nil then
        error(('Unable to decode "%s" as integer'):format(str))
    end
    return res
end

local function decode_float(str)
    -- tonumber() handles 'inf', '-inf', 'nan', '-nan'.
    local res = tonumber(str)
    -- TODO: We possibly should return an error for the following
    -- cases:
    --
    -- * tonumber('-' .. string.rep('9', 1000)) -> -inf
    -- * tonumber(string.rep('9', 1000)) -> inf
    if res == nil then
        error(('Unable to decode "%s" as floating point number'):format(str))
    end
    return res
end

local function decode_string(str)
    return str
end

local function decode(str)
    assert(type(str) == 'string')

    if is_in_tagged_format(str) then
        local tag = str:match('^(!!.-) ')
        if tag == nil then
            error(('Unable to decode "%s": ' ..
                'ill-formed tagged value'):format(str))
        end
        if tag == '!!str' then
            return decode_string(str:sub(7))
        end
        error(('Unable to decode "%s": unknown tag "%s"'):format(str, tag))
    end

    if is_in_null_format(str) then
        return decode_null(str)
    elseif is_in_boolean_format(str) then
        return decode_boolean(str)
    elseif is_in_integer_format(str) then
        return decode_integer(str)
    elseif is_in_integer_hex_format(str) then
        error(('Unable to decode "%s": ' ..
            'integer hex format is not supported yet'):format(str))
    elseif is_in_float_format(str) then
        return decode_float(str)
    elseif is_in_float_hex_format(str) then
        error(('Unable to decode "%s": ' ..
            'float hex format is not supported yet'):format(str))
    end

    return decode_string(str)
end

-- }}} Decoding functions

return {
    encode = encode,
    decode = decode,
}
