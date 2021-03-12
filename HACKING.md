# Hacking

## Make targets

Run luacheck linter:

```sh
$ make lint
```

(The `luacheck` executable is searched in `PATH` and `${DIR}/.rocks/bin` upward
starting from the project directory.)

Run testing:

```sh
$ make test
```

(The same for the `luatest` executable.)

Build the API documentation:

```sh
$ make apidoc
```

(You should have `ldoc` module available and visible for tarantool.)

Build the API documentation and open it in a Web browser:

```sh
$ make serve-apidoc
```

## Deploy API doc

The commands below do not check whether pages are changed.

```sh
git clean -xfd
make apidoc
T=$(mktemp -d)
mv doc/apidoc $T
MSG="$(git log --oneline --no-decorate -1)"
git checkout gh-pages
rm -r *
mv $T/apidoc/* ./
git add --all .
git commit -m "apidoc build: ${MSG}"
git push
git checkout master
```
