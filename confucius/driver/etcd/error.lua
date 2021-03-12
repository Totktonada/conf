local json = require('json')

-- XXX: Add some docs.

-- XXX: Move the whole error hierarchy here?

-- https://github.com/googleapis/googleapis/blob/d652c6370bf66e325da6ac9ad82989fe7ee7bb4b/google/rpc/code.proto
local grpc_errors = {
  [0] = 'OK',
  [1] = 'CANCELLED',
  [2] = 'UNKNOWN',
  [3] = 'INVALID_ARGUMENT',
  [4] = 'DEADLINE_EXCEEDED',
  [5] = 'NOT_FOUND',
  [6] = 'ALREADY_EXISTS',
  [7] = 'PERMISSION_DENIED',
  [8] = 'RESOURCE_EXHAUSTED',
  [9] = 'FAILED_PRECONDITION',
  [10] = 'ABORTED',
  [11] = 'OUT_OF_RANGE',
  [12] = 'UNIMPLEMENTED',
  [13] = 'INTERNAL',
  [14] = 'UNAVAILABLE',
  [15] = 'DATA_LOSS',
  [16] = 'UNAUTHENTICATED',
}

-- {OK = 0, ...}
for code = 0, #grpc_errors do
    local name = grpc_errors[code]
    grpc_errors[name] = code
end

local mt
mt = {
    __concat = function(lhs, rhs)
        if getmetatable(lhs) == mt then
            return tostring(lhs) .. rhs
        elseif getmetatable(rhs) == mt then
            return lhs .. tostring(rhs)
        else
            error('confucius.driver.etcd.error.mt.__concat(): neither of ' ..
                'args is an etcd error')
        end
    end,
    __tostring = function(self)
        return self.message or json.encode(self)
    end,
}

-- <runtimeError>: see [1]. The 'error' field is just copy of
-- 'message' and is present due to compatibility matters. So we
-- dropped the 'error' field here.
--
-- Fun fact: the 'error' field was dropped in grpc-gateway v2, but
-- etcd is on v1 at the moment of writting this (etcd v3.4.15).
-- See [2] for details.
--
-- <runtimeStreamError>: see [3].
--
-- Another fun fact: it seems, <runtimeStreamError> will gone in
-- the future too. See [4] for details.
--
-- luacheck: push max line length 156
-- [1]: https://github.com/etcd-io/etcd/commit/c6fce8c320b144ceed324cd4fc779cfd207d5dbd
-- [2]: https://github.com/grpc-ecosystem/grpc-gateway/pull/1242
-- [3]: https://github.com/etcd-io/etcd/blob/v3.4.15/Documentation/dev-guide/apispec/swagger/rpc.swagger.json#L2603-L2628
-- [4]: https://github.com/grpc-ecosystem/grpc-gateway/pull/1262
-- luacheck: pop
local function new(response)
    local code = response.code or response.grpc_code
    local message = response.message
    local details = response.details
    return setmetatable({
        code = code,
        message = message,
        details = details,
        -- For reading convenience.
        code_name = grpc_errors[code],
    }, mt)
end

return setmetatable(grpc_errors, {
    __index = {
        new = new,
    }
})
