-- Tiny module with constants and predicates around floating-point
-- numbers.

local nan = 0 / 0
local inf = 1 / 0
local ninf = -inf

local function isnan(n)
    assert(type(n) == 'number')
    return n ~= n
end

-- Positive or negative infinity.
local function isinf(n)
    assert(type(n) == 'number')
    local d = n - n
    return not isnan(n) and isnan(d)
end

-- Positive infinity.
local function ispinf(n)
    assert(type(n) == 'number')
    return n > 0 and isinf(n)
end

-- Negative infinity.
local function isninf(n)
    assert(type(n) == 'number')
    return n < 0 and isinf(n)
end

local function isfinite(n)
    assert(type(n) == 'number')
    return not isnan(n) and not isinf(n)
end

local function isinteger(n)
    assert(type(n) == 'number')
    return isfinite(n) and math.floor(n) == n
end

return {
    nan = nan,
    inf = inf,
    ninf = ninf,
    isnan = isnan,
    isinf = isinf,
    ispinf = ispinf,
    isninf = isninf,
    isfinite = isfinite,
    isinteger = isinteger,
}
