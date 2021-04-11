local json = require('json')
local t = require('luatest')
local m = require('conf.math')

local g = t.group()

-- We unable to use isnan(), isinf() and isfinite() from libm
-- using LuaJIT FFI for testing, because they are macros and
-- POSIX does not guarantee that they will be exposed as
-- functions.

local function reference_isnan(n)
    assert(type(n) == 'number')
    return json.encode(n) == 'nan'
end

local function reference_ispinf(n)
    assert(type(n) == 'number')
    return json.encode(n) == 'inf'
end

local function reference_isninf(n)
    assert(type(n) == 'number')
    return json.encode(n) == '-inf'
end

local function reference_isfinite(n)
    assert(type(n) == 'number')
    local enc = json.encode(n)
    -- We should check for the scientific notation too, but the
    -- usual notation is enough for the test ATM.
    return enc:match('^%-?[0-9]+$') or enc:match('^%-?[0-9]+%.[0-9]+$')
end

g.test_math = function()
    -- Verify m.nan.
    t.assert(m.nan ~= m.nan)
    t.assert(reference_isnan(m.nan))
    t.assert(not reference_ispinf(m.nan))
    t.assert(not reference_isninf(m.nan))
    t.assert(not reference_isfinite(m.nan))

    -- Verify m.inf.
    t.assert(reference_isnan(m.inf - m.inf))
    t.assert(m.inf > 0)
    t.assert(not reference_isnan(m.inf))
    t.assert(reference_ispinf(m.inf))
    t.assert(not reference_isninf(m.inf))
    t.assert(not reference_isfinite(m.inf))

    -- Verify m.ninf.
    t.assert(reference_isnan(m.ninf - m.ninf))
    t.assert(m.ninf < 0)
    t.assert(not reference_isnan(m.ninf))
    t.assert(not reference_ispinf(m.ninf))
    t.assert(reference_isninf(m.ninf))
    t.assert(not reference_isfinite(m.ninf))

    -- Verify m.isnan().
    t.assert(m.isnan(0 / 0))
    t.assert(not m.isnan(1 / 0))
    t.assert(not m.isnan(-1 / 0))
    t.assert(not m.isnan(42))
    t.assert(not m.isnan(0))
    t.assert(not m.isnan(-42))
    t.assert(not m.isnan(2.7))
    t.assert(not m.isnan(-2.7))

    -- Verify m.isinf().
    t.assert(not m.isinf(0 / 0))
    t.assert(m.isinf(1 / 0))
    t.assert(m.isinf(-1 / 0))
    t.assert(not m.isinf(42))
    t.assert(not m.isinf(0))
    t.assert(not m.isinf(-42))
    t.assert(not m.isinf(2.7))
    t.assert(not m.isinf(-2.7))

    -- Verify m.ispinf().
    t.assert(not m.ispinf(0 / 0))
    t.assert(m.ispinf(1 / 0))
    t.assert(not m.ispinf(-1 / 0))
    t.assert(not m.ispinf(42))
    t.assert(not m.ispinf(0))
    t.assert(not m.ispinf(-42))
    t.assert(not m.ispinf(2.7))
    t.assert(not m.ispinf(-2.7))

    -- Verify m.isninf().
    t.assert(not m.isninf(0 / 0))
    t.assert(not m.isninf(1 / 0))
    t.assert(m.isninf(-1 / 0))
    t.assert(not m.isninf(42))
    t.assert(not m.isninf(0))
    t.assert(not m.isninf(-42))
    t.assert(not m.isninf(2.7))
    t.assert(not m.isninf(-2.7))

    -- Verify m.isfinite().
    t.assert(not m.isfinite(0 / 0))
    t.assert(not m.isfinite(1 / 0))
    t.assert(not m.isfinite(-1 / 0))
    t.assert(m.isfinite(42))
    t.assert(m.isfinite(0))
    t.assert(m.isfinite(-42))
    t.assert(m.isfinite(2.7))
    t.assert(m.isfinite(-2.7))

    -- Verify m.isinteger().
    t.assert(not m.isinteger(0 / 0))
    t.assert(not m.isinteger(1 / 0))
    t.assert(not m.isinteger(-1 / 0))
    t.assert(m.isinteger(42))
    t.assert(m.isinteger(0))
    t.assert(m.isinteger(-42))
    t.assert(not m.isinteger(2.7))
    t.assert(not m.isinteger(-2.7))
end
