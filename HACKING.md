# Hacking

Run luacheck linter:

```sh
$ make lint
```

(You check have `luacheck` executable in `PATH`.)

Run testing:

```sh
$ make test
```

(You check have `luatest` executable in `PATH`.)

Build the API documentation:

```sh
$ make apidoc
```

(You should have `ldoc` module available and visible for tarantool.)

Build the API documentation and open it in a Web browser:

```sh
$ make serve-apidoc
```
