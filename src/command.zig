const std = @import("std");
const Writer = std.io.Writer;

const Store = @import("store.zig").Store;
const encoding = @import("encoding.zig");
const BulkArray = encoding.BulkArray;
const BulkString = encoding.BulkString;

const OK = "+OK\r\n";
const NULL = "$-1\r\n";
const PONG = "+PONG\r\n";

const ParseError = error{
    InvalidArrayFormat,
    InvalidLength,
    MissingData,
    UnknownCommand,
};

// NOTE: Assume buffer holds a single command in RESP array format.
// Pipelining is technically a thing, but I'm not sure what the format should be...
// It seems like it should be the "inline command" format (not RESP): https://redis.io/docs/latest/develop/reference/protocol-spec/#inline-commands
pub fn parse(alloc: std.mem.Allocator, buf: []const u8) !Command {
    var iter = try ParseIterator.init(alloc, buf);
    defer iter.deinit(alloc);

    // command (first element)
    const cmd_parsed = p: {
        if (iter.next()) |parsed| {
            break :p parsed;
        }

        return ParseError.UnknownCommand;
    };

    // normalize to lower case to check against enum
    const lower = try std.ascii.allocLowerString(alloc, cmd_parsed);
    defer alloc.free(lower);

    const cmd = std.meta.stringToEnum(CommandName, lower) orelse {
        return ParseError.UnknownCommand;
    };

    switch (cmd) {
        .ping => return Command{ .ping = PING{} },
        .echo => return Command{ .echo = try ECHO.parse(&iter) },
        .set => return Command{ .set = try SET.parse(&iter) },
        .get => return Command{ .get = try GET.parse(&iter) },
        .rpush => return Command{ .rpush = try PUSH.parse(alloc, &iter, .right) },
        .lpush => return Command{ .lpush = try PUSH.parse(alloc, &iter, .left) },
        .lrange => return Command{ .lrange = try LRANGE.parse(&iter) },
        .llen => return Command{ .llen = try LLEN.parse(&iter) },
        .lpop => return Command{ .lpop = try LPOP.parse(&iter) },
    }
}

const ParseIterator = struct {
    parsed: std.ArrayList([]const u8),
    index: usize = 0,

    fn init(alloc: std.mem.Allocator, buf: []const u8) !ParseIterator {
        // iterator over raw bulk array buffer
        var iter = std.mem.splitSequence(u8, buf, "\r\n");

        // "parsed" items
        // gather up every bulk string within the array, validating the $<length> component
        // (but not storing them)
        var parsed: std.ArrayList([]const u8) = .empty;
        errdefer parsed.deinit(alloc);

        // parse size (n) of bulk string array
        const n_slice = iter.first();

        // expected: '*<length>'
        if (n_slice.len < 2) {
            return ParseError.InvalidArrayFormat;
        }

        if (n_slice[0] != '*') {
            return ParseError.InvalidArrayFormat;
        }

        // 'n' elements in bulk array,
        // will validate before returning
        const n = try std.fmt.parseInt(usize, n_slice[1..], 10);
        while (iter.next()) |element| {
            if (std.ascii.eqlIgnoreCase(element, "")) {
                // either an empty array, or "end of iteration" edge case (see unit test)
                break;
            }

            // parse length of array element
            if (element.len < 2) {
                return ParseError.InvalidLength;
            }

            if (element[0] != '$') {
                return ParseError.InvalidLength;
            }

            const length = try std.fmt.parseInt(u32, element[1..], 10);
            const data = data: {
                if (iter.next()) |d| {
                    break :data d;
                } else {
                    return ParseError.MissingData;
                }
            };

            if (length != data.len) {
                return ParseError.InvalidLength;
            }

            // all good
            try parsed.append(alloc, data);
        }

        if (parsed.items.len != n) {
            return ParseError.InvalidArrayFormat;
        }

        return .{ .parsed = parsed };
    }

    fn deinit(iter: *ParseIterator, alloc: std.mem.Allocator) void {
        iter.parsed.deinit(alloc);
    }

    fn next(iter: *ParseIterator) ?[]const u8 {
        if (iter.index >= iter.parsed.items.len) {
            return null;
        }

        const current = iter.index;
        iter.index += 1;

        return iter.parsed.items[current];
    }
};

const CommandName = enum {
    ping,
    echo,
    set,
    get,
    rpush,
    lpush,
    lrange,
    llen,
    lpop,
};

const Command = union(CommandName) {
    ping: PING,
    echo: ECHO,
    set: SET,
    get: GET,
    rpush: PUSH,
    lpush: PUSH,
    lrange: LRANGE,
    llen: LLEN,
    lpop: LPOP,

    pub fn do(command: *Command, alloc: std.mem.Allocator, kv: *Store, out: *Writer) !void {
        switch (command.*) {
            .ping => |ping| return ping.do(out),
            .echo => |echo| return echo.do(out),
            .set => |set| return set.do(out, kv),
            .get => |get| return get.do(alloc, out, kv),
            .rpush, .lpush => |push| return push.do(out, kv),
            .lrange => |lrange| return lrange.do(alloc, out, kv),
            .llen => |llen| return llen.do(out, kv),
            .lpop => |lpop| return lpop.do(alloc, out, kv),
        }
    }
};

const PING = struct {
    fn do(_: PING, w: *Writer) !void {
        return w.print(PONG, .{});
    }
};

const ECHO = struct {
    arg: []const u8,

    fn parse(iter: *ParseIterator) !ECHO {
        const arg = iter.next() orelse return ParseError.MissingData;
        return .{ .arg = arg };
    }

    fn do(cmd: ECHO, out: *Writer) !void {
        return BulkString.encode(out, cmd.arg);
    }
};

const SET = struct {
    key: []const u8,
    value: []const u8,
    expires_in: ?i64 = null,

    fn parse(iter: *ParseIterator) !SET {
        const key = iter.next() orelse return ParseError.MissingData;
        const value = iter.next() orelse return ParseError.MissingData;

        // check for expiry value
        const px = iter.next();
        if (px) |_| {
            // expect "PX <expiry>"
            const expires_str = iter.next() orelse return ParseError.MissingData;
            const expires = try std.fmt.parseInt(i64, expires_str, 10);

            return .{ .key = key, .value = value, .expires_in = expires };
        }

        return .{ .key = key, .value = value };
    }

    fn do(cmd: SET, out: *Writer, kv: *Store) !void {
        try kv.set(cmd.key, cmd.value, .{ .expires_in = cmd.expires_in });
        return out.print(OK, .{});
    }
};

const GET = struct {
    key: []const u8,

    fn parse(iter: *ParseIterator) !GET {
        const key = iter.next() orelse return ParseError.MissingData;
        return .{ .key = key };
    }

    fn do(
        cmd: GET,
        alloc: std.mem.Allocator,
        out: *Writer,
        kv: *Store,
    ) !void {
        // TODO(nickmonad) if the store can only be used in a single-threaded context
        // moving forward, I don't think we actually need to allocate into the alloc here...
        // since we can simply copy the stable value in the pointer returned by the store to the output
        const value = try kv.get(alloc, cmd.key);
        if (value) |v| {
            defer alloc.free(v.value);
            return BulkString.encode(out, v.value);
        }

        return out.print(NULL, .{});
    }
};

const PUSH = struct {
    list: []const u8,
    elements: std.ArrayList([]const u8),
    direction: Direction,

    const Direction = enum { left, right };

    fn parse(
        alloc: std.mem.Allocator,
        iter: *ParseIterator,
        direction: Direction,
    ) !PUSH {
        const list = iter.next() orelse return ParseError.MissingData;
        var elements: std.ArrayList([]const u8) = .empty;

        while (iter.next()) |element| {
            try elements.append(alloc, element);
        }

        return .{ .list = list, .elements = elements, .direction = direction };
    }

    fn do(cmd: PUSH, out: *Writer, kv: *Store) !void {
        const length = length: {
            var len: usize = undefined;
            for (cmd.elements.items) |element| {
                // TODO: probably gonna be more efficient to add an kv method
                // for rpushing multiple elements, so we'd don't have to lookup the key every time
                len = len: switch (cmd.direction) {
                    .left => break :len try kv.lpush(cmd.list, element),
                    .right => break :len try kv.rpush(cmd.list, element),
                };
            }

            break :length len;
        };

        return out.print(":{d}\r\n", .{length});
    }
};

const LRANGE = struct {
    list: []const u8,
    start: isize,
    stop: isize,

    fn parse(iter: *ParseIterator) !LRANGE {
        const list = iter.next() orelse return ParseError.MissingData;
        const start = try std.fmt.parseInt(isize, iter.next() orelse return ParseError.MissingData, 10);
        const stop = try std.fmt.parseInt(isize, iter.next() orelse return ParseError.MissingData, 10);

        return .{
            .list = list,
            .start = start,
            .stop = stop,
        };
    }

    fn do(
        cmd: LRANGE,
        alloc: std.mem.Allocator,
        out: *Writer,
        kv: *Store,
    ) !void {
        // TODO(nicmonad) again, if the Store is going to be single-threaded, we don't need
        // this extra allocation into the alloc...
        var list = try kv.lrange(alloc, cmd.list, cmd.start, cmd.stop);
        defer list.deinit(alloc);

        var to_encode: std.ArrayList([]const u8) = .empty;
        defer to_encode.deinit(alloc);

        for (list.list.items) |item| {
            try to_encode.append(alloc, item.value);
        }

        return BulkArray.encode(out, to_encode.items);
    }
};

const LLEN = struct {
    list: []const u8,

    fn parse(iter: *ParseIterator) !LLEN {
        const list = iter.next() orelse return ParseError.MissingData;
        return .{ .list = list };
    }

    fn do(cmd: LLEN, out: *Writer, kv: *Store) !void {
        const length = kv.llen(cmd.list);
        return out.print(":{d}\r\n", .{length});
    }
};

const LPOP = struct {
    key: []const u8,
    count: usize = 1,

    fn parse(iter: *ParseIterator) !LPOP {
        const key = iter.next() orelse return ParseError.MissingData;

        if (iter.next()) |count| {
            const c = try std.fmt.parseInt(usize, count, 10);
            return .{ .key = key, .count = c };
        }

        return .{ .key = key };
    }

    fn do(
        cmd: LPOP,
        alloc: std.mem.Allocator,
        out: *Writer,
        kv: *Store,
    ) !void {
        var list = try kv.lpop(alloc, cmd.key, cmd.count) orelse {
            return out.print(NULL, .{});
        };

        defer list.deinit(alloc);

        if (list.list.items.len == 0) {
            return BulkString.encode(out, "");
        }

        if (list.list.items.len == 1) {
            // only 1 element, return as bulk string
            const element = list.list.items[0];
            return BulkString.encode(out, element.value);
        }

        // multiple elements
        var to_encode: std.ArrayList([]const u8) = .empty;
        defer to_encode.deinit(alloc);

        for (list.list.items) |item| {
            try to_encode.append(alloc, item.value);
        }

        return BulkArray.encode(out, to_encode.items);
    }
};

// tests

// test helper
// create a BulkArray, assuming the std.testing.allocator (ignoring allocation errors)
// usage: BA(&.{ "ECHO", "test" })
// Don't forget to free the returned .str
fn BA(w: *Writer, elements: []const []const u8) void {
    return BulkArray.encode(w, elements) catch unreachable;
}

test "splitSequence sanity check" {
    const data = "*2\r\n$5\r\nhello\r\n$3\r\nzig\r\n";
    var iter = std.mem.splitSequence(u8, data, "\r\n");

    try std.testing.expectEqualStrings("*2", iter.next().?);
    try std.testing.expectEqualStrings("$5", iter.next().?);
    try std.testing.expectEqualStrings("hello", iter.next().?);
    try std.testing.expectEqualStrings("$3", iter.next().?);
    try std.testing.expectEqualStrings("zig", iter.next().?);
    try std.testing.expectEqualStrings("", iter.next().?); // gotcha!
    try std.testing.expect(iter.next() == null);
    try std.testing.expect(iter.next() == null);
}

test "ParseIterator" {
    const alloc = std.testing.allocator;
    var iter = try ParseIterator.init(alloc, "*1\r\n$4\r\nPING\r\n");
    defer iter.deinit(alloc);

    try std.testing.expectEqualSlices(u8, "PING", iter.next().?);
    try std.testing.expectEqual(null, iter.next());
}

test "ParseIterator termination" {
    const alloc = std.testing.allocator;
    var iter = try ParseIterator.init(alloc, "*1\r\n$4\r\nPING\r\n");
    defer iter.deinit(alloc);

    try std.testing.expectEqualSlices(u8, "PING", iter.next().?);

    for (0..10) |_| {
        // this thing had better be null!
        try std.testing.expectEqual(null, iter.next());
    }
}

test "ParseIterator multiple elements" {
    const alloc = std.testing.allocator;
    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);

    BA(&w, &.{ "SET", "test", "zig" });

    var iter = try ParseIterator.init(alloc, w.buffered());
    defer iter.deinit(alloc);

    try std.testing.expectEqualSlices(u8, "SET", iter.next().?);
    try std.testing.expectEqualSlices(u8, "test", iter.next().?);
    try std.testing.expectEqualSlices(u8, "zig", iter.next().?);
    try std.testing.expectEqual(null, iter.next());
}

test "ParseIterator invalid" {
    const alloc = std.testing.allocator;
    // no need to deinit(), should be done by iterator on failure

    try std.testing.expectEqual(ParseError.InvalidArrayFormat, ParseIterator.init(alloc, "1\r\n$4\r\nPING\r\n"));
    try std.testing.expectEqual(ParseError.InvalidArrayFormat, ParseIterator.init(alloc, ")1\r\n$4\r\nPING\r\n"));
    try std.testing.expectEqual(ParseError.InvalidLength, ParseIterator.init(alloc, "*1\r\n$3\r\nPING\r\n"));
    try std.testing.expectEqual(ParseError.InvalidLength, ParseIterator.init(alloc, "*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$0\r\nzig\r\n"));
    try std.testing.expectEqual(ParseError.MissingData, ParseIterator.init(alloc, "*1\r\n$3"));
}

test "parse invalid" {
    const alloc = std.testing.allocator;
    try std.testing.expectEqual(ParseError.InvalidArrayFormat, parse(alloc, ""));
    try std.testing.expectEqual(ParseError.InvalidArrayFormat, parse(alloc, "1\r\n$4\r\nPING\r\n"));
    try std.testing.expectEqual(ParseError.InvalidArrayFormat, parse(alloc, ")1\r\n$4\r\nPING\r\n"));
    try std.testing.expectEqual(ParseError.UnknownCommand, parse(alloc, "*1\r\n$3\r\nZIG\r\n"));
}

test "parse PING" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{"PING"});

    const alloc = arena.allocator();
    const parsed = try parse(alloc, w.buffered());
    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.ping);
}

test "parse ECHO error missing" {
    const alloc = std.testing.allocator;

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{"ECHO"});

    try std.testing.expectError(ParseError.MissingData, parse(alloc, w.buffered()));
}

test "parse ECHO arg" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "ECHO", "hello, zig" });

    const parsed = try parse(alloc, w.buffered());
    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.echo);
    try std.testing.expectEqualSlices(u8, "hello, zig", parsed.echo.arg);
}

test "parse SET error missing" {
    const alloc = std.testing.allocator;

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "SET", "test" });

    try std.testing.expectError(ParseError.MissingData, parse(alloc, w.buffered()));
}

test "parse SET key value" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "SET", "test", "zig" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.set);
    try std.testing.expectEqualSlices(u8, "test", parsed.set.key);
    try std.testing.expectEqualSlices(u8, "zig", parsed.set.value);
}

test "parse SET key value PX expiry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "SET", "test", "zig", "PX", "100" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.set);
    try std.testing.expectEqualSlices(u8, "test", parsed.set.key);
    try std.testing.expectEqualSlices(u8, "zig", parsed.set.value);
    try std.testing.expectEqual(100, parsed.set.expires_in.?);
}

test "parse GET" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "GET", "test" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.get);
    try std.testing.expectEqualSlices(u8, "test", parsed.get.key);
}

test "parse RPUSH, 1 element" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "RPUSH", "test", "zig" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.rpush);
    try std.testing.expectEqualSlices(u8, "test", parsed.rpush.list);

    try std.testing.expectEqual(1, parsed.rpush.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", parsed.rpush.elements.items[0]);
}

test "parse RPUSH, mulitple elements" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "RPUSH", "test", "zig", "cool" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.rpush);
    try std.testing.expectEqualSlices(u8, "test", parsed.rpush.list);

    try std.testing.expectEqual(2, parsed.rpush.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", parsed.rpush.elements.items[0]);
    try std.testing.expectEqualSlices(u8, "cool", parsed.rpush.elements.items[1]);
}

test "parse LPUSH, 1 element" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LPUSH", "test", "zig" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.lpush);
    try std.testing.expectEqualSlices(u8, "test", parsed.lpush.list);

    try std.testing.expectEqual(1, parsed.lpush.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", parsed.lpush.elements.items[0]);
}

test "parse LRANGE" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LRANGE", "test", "0", "1" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.lrange);
    try std.testing.expectEqualSlices(u8, "test", parsed.lrange.list);
    try std.testing.expectEqual(0, parsed.lrange.start);
    try std.testing.expectEqual(1, parsed.lrange.stop);
}

test "parse LRANGE negative" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LRANGE", "test", "10", "-5" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.lrange);
    try std.testing.expectEqualSlices(u8, "test", parsed.lrange.list);
    try std.testing.expectEqual(10, parsed.lrange.start);
    try std.testing.expectEqual(-5, parsed.lrange.stop);
}

test "parse LLEN" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LLEN", "test" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.llen);
    try std.testing.expectEqualSlices(u8, "test", parsed.llen.list);
}

test "parse LPOP" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LPOP", "test", "10" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.lpop);
    try std.testing.expectEqualSlices(u8, "test", parsed.lpop.key);
    try std.testing.expectEqual(10, parsed.lpop.count);
}
