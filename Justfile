flox:
    flox activate -- fish

build:
    zig build

run:
    zig build run

test:
    zig build test --summary all

test-cli:
    @rm -f test.cli.out
    @redis-cli PING >> test.cli.out
    @redis-cli ECHO zig >> test.cli.out
    @redis-cli SET test zig >> test.cli.out
    @redis-cli GET test >> test.cli.out
    @redis-cli SET expires test PX 1000 >> test.cli.out
    @redis-cli GET expires >> test.cli.out
    @sleep 1.1s
    @redis-cli GET expires >> test.cli.out

    @redis-cli RPUSH list test >> test.cli.out
    @redis-cli RPUSH list again >> test.cli.out
    @redis-cli RPUSH list foo bar >> test.cli.out
    @redis-cli LRANGE list 0 1 >> test.cli.out
    @redis-cli LRANGE list 0 3 >> test.cli.out
    @redis-cli LPOP list >> test.cli.out
    @redis-cli LPOP list 2 >> test.cli.out

    @redis-cli LPUSH foo a >> test.cli.out
    @redis-cli LPUSH foo b >> test.cli.out
    @redis-cli LPUSH foo c >> test.cli.out
    @redis-cli LRANGE foo 0 2 >> test.cli.out
    @redis-cli LLEN foo >> test.cli.out
    @redis-cli LPOP foo >> test.cli.out
    diff test.cli.out test.cli.expected
