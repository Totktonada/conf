local ffi = require('ffi')
local t = require('luatest')
local s = require('conf.client.etcd.scalar_serializer')

local g = t.group()

-- Sure, it is possible to coalesce most of
-- encode / decode / marshalling test cases,
-- but it is hard for me to think about three
-- different actions at the same time.
--
-- So there is some level of testing code duplication and
-- it is intentional. The intention is to make the code
-- simpler.

-- {{{ Encoding

g.test_encode = function()
    local function check(value, exp)
        t.assert_equals(s.encode(value), exp)
    end

    -- Null.
    check(nil, 'null')
    check(box.NULL, 'null')

    -- Boolean.
    check(false, 'false')
    check(true, 'true')

    -- Integer.
    check(-9223372036854775808LL, '-9223372036854775808') -- -2^63
    check(-42LL, '-42')
    check(-42, '-42')
    check(0, '0')
    check(0LL, '0')
    check(0ULL, '0')
    check(42, '42')
    check(42LL, '42')
    check(42ULL, '42')
    check(9223372036854775807LL, '9223372036854775807') -- 2^63-1
    check(9223372036854775807ULL, '9223372036854775807') -- 2^63-1
    check(18446744073709551615ULL, '18446744073709551615') -- 2^64-1

    -- Float.
    check(-1e+20, '-1e+20')
    check(-9007199254740994, '-9.007199254741e+15') -- -2^53-2
    check(-0.5, '-0.5')
    check(-0.0001, '-0.0001')
    check(-0.00001, '-1e-05')
    -- No way to encode zero (generally speaking, any number
    -- within the [-10^14 + 1; 10^14 - 1] range without a
    -- fractional part) as a floating point value from the Lua
    -- client. However it does not matter for marshalling
    -- from / to Lua, because we'll get a <number> Lua value
    -- back for this range despite that it is encoded as integer.
    check(0.0001, '0.0001')
    check(0.5, '0.5')
    check(0.00001, '1e-05')
    check(9007199254740994, '9.007199254741e+15') -- 2^53+2
    check(1e+20, '1e+20')
    check(1/0, 'inf')
    check(-1/0, '-inf')
    check(0/0, 'nan')
    check(-0/0, 'nan')

    -- Float / integer watershed.
    check(-100000000000000, '-1e+14')         -- -10^14     -> float
    check(-99999999999999, '-99999999999999') -- -10^14 + 1 -> int
    check(99999999999999, '99999999999999')   --  10^14 - 1 -> int
    check(100000000000000, '1e+14')           --  10^14     -> float

    -- String.
    check('foo', 'foo')
    check('', '')
    check('\0', '\0')
    check('\0\0\0', '\0\0\0')
    check('\t\r\n', '\t\r\n')
    -- Unicode (correct UTF-8, incorrect UTF-8).
    check('привет', 'привет')
    check('\xc0\x00', '\xc0\x00')
    -- A string that looks like a tagged value.
    check('!!x', '!!str !!x')
    check('!!str x', '!!str !!str x')
    -- Doesn't look as a tag.
    check('x!!', 'x!!')
    check('x !!str x', 'x !!str x')
    -- Like a null.
    check('null', '!!str null')
    -- Like a boolean.
    check('true', '!!str true')
    check('false', '!!str false')
    -- Like an integer.
    check('-42', '!!str -42')
    check('42', '!!str 42')
    -- Like an integer in hex.
    check('-0xdeadbeef', '!!str -0xdeadbeef')
    check('0xdeadbeef', '!!str 0xdeadbeef')
    check('-0xDEADBEEF', '!!str -0xDEADBEEF')
    check('0xDEADBEEF', '!!str 0xDEADBEEF')
    -- With capital X.
    check('-0Xdeadbeef', '!!str -0Xdeadbeef')
    check('0Xdeadbeef', '!!str 0Xdeadbeef')
    check('-0XDEADBEEF', '!!str -0XDEADBEEF')
    check('0XDEADBEEF', '!!str 0XDEADBEEF')
    -- Like a floating point number.
    check('-4.5', '!!str -4.5')
    check('4.5', '!!str 4.5')
    -- Like the scientific notation.
    check('-1e1', '!!str -1e1')
    check('1e1', '!!str 1e1')
    check('-1.4e1', '!!str -1.4e1')
    check('1.4e1', '!!str 1.4e1')
    -- With a negative exponent.
    check('-1e-1', '!!str -1e-1')
    check('1e-1', '!!str 1e-1')
    check('-1.4e-1', '!!str -1.4e-1')
    check('1.4e-1', '!!str 1.4e-1')
    -- With the explicit plus in the exponent.
    check('-1e+1', '!!str -1e+1')
    check('1e+1', '!!str 1e+1')
    check('-1.4e+1', '!!str -1.4e+1')
    check('1.4e+1', '!!str 1.4e+1')
    -- With capital E.
    check('-1E-1', '!!str -1E-1')
    check('1E-1', '!!str 1E-1')
    check('-1E1', '!!str -1E1')
    check('1E1', '!!str 1E1')
    check('-1E+1', '!!str -1E+1')
    check('1E+1', '!!str 1E+1')
    check('-1.5E-1', '!!str -1.5E-1')
    check('1.5E-1', '!!str 1.5E-1')
    check('-1.5E1', '!!str -1.5E1')
    check('1.5E1', '!!str 1.5E1')
    check('-1.5E+1', '!!str -1.5E+1')
    check('1.5E+1', '!!str 1.5E+1')
    -- Like a floating point value in hex.
    check('-0x1.f', '!!str -0x1.f')
    check('0x1.f', '!!str 0x1.f')
    -- With capital X / [A-F].
    check('-0X1.f', '!!str -0X1.f')
    check('0X1.f', '!!str 0X1.f')
    check('-0x1.F', '!!str -0x1.F')
    check('0x1.F', '!!str 0x1.F')
    -- Like a floating point value in hex and scientific notation.
    check('-0x1p1', '!!str -0x1p1')
    check('0x1p1', '!!str 0x1p1')
    check('-0xfp+1', '!!str -0xfp+1')
    check('0xfp+1', '!!str 0xfp+1')
    check('-0xfp-1', '!!str -0xfp-1')
    check('0xfp-1', '!!str 0xfp-1')
    check('-0x1.fp1', '!!str -0x1.fp1')
    check('0x1.fp1', '!!str 0x1.fp1')
    check('-0x1.fp+1', '!!str -0x1.fp+1')
    check('0x1.fp+1', '!!str 0x1.fp+1')
    check('-0x1.fp-1', '!!str -0x1.fp-1')
    check('0x1.fp-1', '!!str 0x1.fp-1')
    -- With capital X / [A-F] / P.
    check('-0X1P1', '!!str -0X1P1')
    check('0X1P1', '!!str 0X1P1')
    check('-0XFP+1', '!!str -0XFP+1')
    check('0XFP+1', '!!str 0XFP+1')
    check('-0XFP-1', '!!str -0XFP-1')
    check('0XFP-1', '!!str 0XFP-1')
    check('-0X1.FP1', '!!str -0X1.FP1')
    check('0X1.FP1', '!!str 0X1.FP1')
    check('-0X1.FP+1', '!!str -0X1.FP+1')
    check('0X1.FP+1', '!!str 0X1.FP+1')
    check('-0X1.FP-1', '!!str -0X1.FP-1')
    check('0X1.FP-1', '!!str 0X1.FP-1')
    -- Like a special floating point value.
    check('inf', '!!str inf')
    check('-inf', '!!str -inf')
    check('nan', '!!str nan')
    check('-nan', '!!str -nan')
    -- Starts from minus, but doesn't look like a number.
    check('-foo', '-foo')
    -- Starts from a number, but doesn't look like a number.
    check('0day', '0day')
    check('0.', '0.')
    -- Starts from a period.
    check('.1', '.1')
    -- Similar to the scientific notation, but doesn't adhere our
    -- formal rules.
    check('1e', '1e')
    check('1e+', '1e+')
    check('1e-', '1e-')
    check('1.e1', '1.e1')
    check('.1e1', '.1e1')
    -- Starts from '0x' or '0X', but doesn't adhere the hex number
    -- format.
    check('-0x', '-0x')
    check('0x', '0x')
    check('-0x0x', '-0x0x')
    check('0x0x', '0x0x')
    -- Starts from plus.
    check('+1', '+1')
    -- Like tostring(<number64>).
    check('-1LL', '-1LL')
    check('1LL', '1LL')
    check('1ULL', '1ULL')
end

g.test_encode_failure = function()
    local function check(value, exp_err)
        local ok, err = pcall(s.encode, value)
        t.assert(not ok)
        local err_msg = tostring(err):gsub('^.-:.-: ', '')
        t.assert_equals(err_msg, exp_err)
    end

    check(function() end, 'No encoder for function')
    check(ffi.new('char'), 'No encoder for ctype<char>')
end

-- }}} Encoding

-- {{{ Decoding

g.test_decode = function()
    local function check(str, exp)
        local res = s.decode(str)
        t.assert_equals(type(res), type(exp))
        if type(res) == 'cdata' and type(exp) == 'cdata' then
            t.assert_equals(ffi.typeof(res), ffi.typeof(exp))
        end
        t.assert_equals(res, exp)
    end

    -- Null.
    check('null', box.NULL)

    -- Boolean.
    check('false', false)
    check('true', true)

    -- Integer.
    check('-9223372036854775808', -9223372036854775808LL) -- -2^63
    check('-42', -42)
    check('-0', 0)
    check('0', 0)
    check('42', 42)
    check('9223372036854775807', 9223372036854775807ULL) -- 2^63-1
    check('18446744073709551615', 18446744073709551615ULL) -- 2^64-1
    -- Leading zero.
    check('-00', 0)
    check('00', 0)
    check('-09', -9)
    check('09', 9)

    -- Number / number64 watershed.
    check('-100000000000000', -100000000000000LL) -- -10^14     -> number64
    check('-99999999999999', -99999999999999)     -- -10^14 + 1 -> number
    check('99999999999999', 99999999999999)       --  10^14 - 1 -> number
    check('100000000000000', 100000000000000ULL)  --  10^14     -> number64

    -- Float.
    check('-9.007199254740994e+15', -9007199254740994) -- -2^53-2
    check('-1234567890.42', -1234567890.42)
    check('-1.5', -1.5)
    check('-0.5', -0.5)
    check('-0.0001', -0.0001)
    check('-0.00001', -0.00001)
    check('-0.0', 0)
    check('0.0', 0)
    check('0.00001', 0.00001)
    check('0.0001', 0.0001)
    check('0.5', 0.5)
    check('1.5', 1.5)
    check('1234567890.42', 1234567890.42)
    check('9.007199254740994e+15', 9007199254740994) -- 2^53+2
    -- Scientific notation.
    check('-1e20', -1e20)
    check('-1e1', -10)
    check('-1e0', -1)
    check('0e0', 0)
    check('1e0', 1)
    check('1e1', 10)
    check('1e20', 1e20)
    -- In the scientific notation with a decimal period.
    check('-1.5e1', -15)
    check('1.5e1', 15)
    -- With a negative exponent.
    check('-1e-1', -0.1)
    check('1e-1', 0.1)
    check('-1.5e-1', -0.15)
    check('1.5e-1', 0.15)
    -- With the explicit plus in the exponent.
    check('-1e+1', -10)
    check('1e+1', 10)
    check('-1.5e+1', -15)
    check('1.5e+1', 15)
    -- With capital E.
    check('-1E-1', -0.1)
    check('1E-1', 0.1)
    check('-1E1', -10)
    check('1E1', 10)
    check('-1E+1', -10)
    check('1E+1', 10)
    check('-1.5E-1', -0.15)
    check('1.5E-1', 0.15)
    check('-1.5E1', -15)
    check('1.5E1', 15)
    check('-1.5E+1', -15)
    check('1.5E+1', 15)
    -- Scientific notation of a value out of the
    -- [-10^14 + 1, 10^14 - 1] range without a fractional part.
    -- The decoder interpret a value in the scientific notation
    -- as a floating point value, so the result must be <number>,
    -- not <number64>.
    check('-1e14', -100000000000000) -- -10^14
    check('1e14', 100000000000000)   --  10^14
    -- Special values.
    check('inf', 1/0)
    check('-inf', -1/0)
    local res = s.decode('nan')
    t.assert(res ~= res)
    local res = s.decode('-nan')
    t.assert(res ~= res)
    -- Leading zero.
    check('-00.0', 0)
    check('00.0', 0)
    check('-00.1', -0.1)
    check('00.1', 0.1)

    -- String.
    check('foo', 'foo')
    check('', '')
    check('\0', '\0')
    check('\0\0\0', '\0\0\0')
    check('\t\r\n', '\t\r\n')
    -- Unicode (correct UTF-8, incorrect UTF-8).
    check('привет', 'привет')
    check('\xc0\x00', '\xc0\x00')
    -- Tagged.
    check('!!str foo', 'foo')
    check('!!str !!str', '!!str')
    -- Untagged, but with !! in a middle.
    check('x!!', 'x!!')
    check('x !!str x', 'x !!str x')
    -- Tagged with a value like null / boolean / integer / float.
    check('!!str null', 'null')
    check('!!str true', 'true')
    check('!!str false', 'false')
    check('!!str -42', '-42')
    check('!!str -1.4', '-1.4')
    check('!!str -1e-4', '-1e-4')
    check('!!str 0', '0')
    check('!!str 1e-4', '1e-4')
    check('!!str 1.4', '1.4')
    check('!!str 42', '42')
    -- Tagged and like a special floating point value.
    check('!!str inf', 'inf')
    check('!!str -inf', '-inf')
    check('!!str nan', 'nan')
    check('!!str -nan', '-nan')
    -- Starts from minus, but doesn't look like a number.
    check('-foo', '-foo')
    -- Starts from a number, but doesn't look like a number.
    check('0day', '0day')
    check('0.', '0.')
    -- Starts from a period.
    check('.1', '.1')
    -- Similar to the scientific notation, but doesn't adhere our
    -- formal rules.
    check('1e', '1e')
    check('1e+', '1e+')
    check('1e-', '1e-')
    check('1.e1', '1.e1')
    check('.1e1', '.1e1')
    -- Starts from '0x' or '0X', but doesn't adhere the hex number
    -- format.
    check('-0x', '-0x')
    check('0x', '0x')
    check('-0x0x', '-0x0x')
    check('0x0x', '0x0x')
    -- Starts from plus.
    check('+1', '+1')
    -- Like tostring(<number64>).
    check('-1LL', '-1LL')
    check('1LL', '1LL')
    check('1ULL', '1ULL')
end

g.test_decode_failure = function()
    local function check(str, exp_err)
        if exp_err:find('%%s') then
            exp_err = exp_err:format(str)
        end
        local ok, err = pcall(s.decode, str)
        t.assert(not ok)
        local err_msg = tostring(err):gsub('^.-:.-: ', '')
        t.assert_equals(err_msg, exp_err)
    end

    -- Integers out of the [-2^63; 2^64 - 1] range.
    local exp_err = 'Unable to decode "%s" as integer'
    check('-9223372036854775809', exp_err) -- -2^63 - 1
    check('18446744073709551616', exp_err) -- 2^64

    -- Ill-formed tagged value.
    local exp_err = 'Unable to decode "%s": ill-formed tagged value'
    check('!!', exp_err)
    check('!!x', exp_err)

    -- Unknown tag.
    check('!!foo x', 'Unable to decode "!!foo x": unknown tag "!!foo"')

    -- Integer in hex (reserved).
    local exp_err =
        'Unable to decode "%s": integer hex format is not supported yet'
    check('-0xdeadbeef', exp_err)
    check('0xdeadbeef', exp_err)
    check('-0xDEADBEEF', exp_err)
    check('0xDEADBEEF', exp_err)
    -- With capital X.
    check('-0Xdeadbeef', exp_err)
    check('0Xdeadbeef', exp_err)
    check('-0XDEADBEEF', exp_err)
    check('0XDEADBEEF', exp_err)

    -- Float in hex (reserved).
    local exp_err =
        'Unable to decode "%s": float hex format is not supported yet'
    check('-0x1p1', exp_err)
    check('0x1p1', exp_err)
    check('-0xfp+1', exp_err)
    check('0xfp+1', exp_err)
    check('-0xfp-1', exp_err)
    check('0xfp-1', exp_err)
    check('-0x1.fp1', exp_err)
    check('0x1.fp1', exp_err)
    check('-0x1.fp+1', exp_err)
    check('0x1.fp+1', exp_err)
    check('-0x1.fp-1', exp_err)
    check('0x1.fp-1', exp_err)
    -- With capital X / [A-F] / P.
    check('-0X1P1', exp_err)
    check('0X1P1', exp_err)
    check('-0XFP+1', exp_err)
    check('0XFP+1', exp_err)
    check('-0XFP-1', exp_err)
    check('0XFP-1', exp_err)
    check('-0X1.FP1', exp_err)
    check('0X1.FP1', exp_err)
    check('-0X1.FP+1', exp_err)
    check('0X1.FP+1', exp_err)
    check('-0X1.FP-1', exp_err)
    check('0X1.FP-1', exp_err)
end

-- }}} Decoding

-- {{{ Marshalling

g.test_marshalling = function()
    -- It is implicitly tested by the encode / decode test cases.
    --
    -- However I written several simple marshalling test cases
    -- explicitly just to verify that nothing was forgotten.

    local function check(src)
        local str = s.encode(src)
        t.assert_equals(type(str), 'string')
        local res = s.decode(str)
        t.assert_equals(type(res), type(src))
        if type(res) == 'cdata' and type(src) == 'cdata' then
            t.assert_equals(ffi.typeof(res), ffi.typeof(src))
        end
        if src ~= src then
            -- NaN.
            t.assert(res ~= res)
        else
            t.assert_equals(res, src)
        end
    end

    -- Null.
    check(box.NULL)

    -- Boolean.
    check(false)
    check(true)

    -- Integer.
    check(-9223372036854775808LL) -- -2^63
    check(-100000000000000LL)     -- -10^14
    check(-99999999999999)        -- -10^14 + 1
    check(-42)
    check(0)
    check(42)
    check(99999999999999)          -- 10^14-1
    check(100000000000000ULL)      -- 10^14
    check(9223372036854775807ULL)  -- 2^63 - 1
    check(18446744073709551615ULL) -- 2^64 - 1

    -- Float.
    check(-1234567890.42)
    check(-1.5)
    check(-0.5)
    check(-0.0001)
    check(-0.00001)
    check(0.00001)
    check(0.0001)
    check(0.5)
    check(1.5)
    check(1234567890.42)
    -- Special floats.
    check(1/0)
    check(-1/0)
    check(0/0)

    -- String.
    check('foo')
    check('')
    check('\0')
    check('\0\0\0')
    check('\t\r\n')
    -- Unicode (correct UTF-8, incorrect UTF-8).
    check('привет')
    check('\xc0\x00')
    -- With !! at beginning.
    check('!!')
    check('!! ')
    check('!!x')
    check('!!str')
    check('!!str x')
    -- Like null / boolean / integer / float.
    check('null')
    check('true')
    check('false')
    check('-42')
    check('-1.4')
    check('-1e-4')
    check('0')
    check('1e-4')
    check('1.4')
    check('42')
    -- Like an integer / float in hex.
    check('-0xdeadbeef')
    check('0XDEADBEEF')
    check('-0x1.f')
    check('0x1.F')
    check('-0x1p1')
    check('0X1.fP-1')
    -- Like a special floating point value.
    check('inf')
    check('-inf')
    check('nan')
    check('-nan')
    -- Starts from minus, but doesn't look like a number.
    check('-foo')
    -- Starts from a number, but doesn't look like a number.
    check('0day')
    check('0.')
    -- Starts from a period.
    check('.1', '.1')
    -- Similar to the scientific notation, but doesn't adhere our
    -- formal rules.
    check('1e', '1e')
    check('1e+', '1e+')
    check('1e-', '1e-')
    check('1.e1', '1.e1')
    check('.1e1', '.1e1')
    -- Starts from '0x' or '0X', but doesn't adhere the hex number
    -- format.
    check('-0x', '-0x')
    check('0x', '0x')
    check('-0x0x', '-0x0x')
    check('0x0x', '0x0x')
    -- Starts from plus.
    check('+1', '+1')
    -- Like tostring(<number64>).
    check('-1LL', '-1LL')
    check('1LL', '1LL')
    check('1ULL', '1ULL')
end

g.test_marshalling_surprises = function()
    -- The encoding / decoding rules is a set of compromises to
    -- make the behaviour in the typical cases as intuitive as
    -- possible. Anyway, there are cases, when we got something
    -- different from a source value after marshalling.

    local function check(src, exp)
        -- Encode and decode back.
        local str = s.encode(src)
        t.assert_equals(type(str), 'string')
        local res = s.decode(str)

        -- Verify the result against 'exp'.
        t.assert_equals(type(res), type(exp))
        if type(res) == 'cdata' and type(exp) == 'cdata' then
            t.assert_equals(ffi.typeof(res), ffi.typeof(exp))
        end
        if exp ~= exp then
            -- NaN.
            t.assert(res ~= res)
        else
            t.assert_equals(res, exp)
        end
    end

    -- Nil.
    check(nil, box.NULL)

    -- Number64.
    check(-1LL, -1)
    check(1LL, 1)
    check(1ULL, 1)
    check(100000000000000LL, 100000000000000ULL) -- 10^14

    -- Precision loss.
    check(1125899906842624, 1125899906842600) -- 2^50
end

-- }}} Marshalling
