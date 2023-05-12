# Potamic

Potamic is a message queue implementation over Redis' Stream data type. It makes it simple to read and write as multiple consumers, automatically tracking "read", "pending" and "processed" messages.

### Index

1. [Get Potamic](#get-potamic)
2. [Quickstart](#quickstart)
3. [Docs](#docs)
4. [Running Locally](#usage-locally)
5. [License](#license)

<a name="get-potamic"></a>
# Get Potamic

### deps.edn

> _TODO_: Publish to Clojars

### Leiningen

> _TODO_: Publish to Clojars

<a name="quickstart"></a>
# Quickstart

> _TODO_: add quickstart

<a name="docs"></a>
# Docs

### Online

[https://chadangelelli.github.io/potamic](https://chadangelelli.github.io/potamic)

### Local

```shell
git clone https://github.com/chadangelelli/potamic.git

cd potamic

open docs/index.html
```

<a name="running-locally"></a>
# Running Locally

### Testing

You'll need 2 terminals open; 1 for Redis and 1 for the test runner.

**Redis:**

```shell
cd /path/to/potamic

bin/db
```

**Test Runner:**

```shell
cd /path/to/potamic

bin/test
# or
bin/test --watch
```

### REPL

```shell
cd /path/to/potamic

bin/repl
```

### Generating Docs

```shell
cd /path/to/potamic

bin/doc
```

<a name="license"></a>
# License

Distributed under the [EPL v1.0](https://raw.githubusercontent.com/chadangelelli/potamic/main/LICENSE) (same as Clojure).

Copyright © 2023 Chad Angelelli
