const std = @import("std");
const assert = std.debug.assert;
const Writer = std.io.Writer;

const Config = @import("config.zig").Config;

const Store = @import("store.zig").Store;
const ListItem = @import("store.zig").ListItem;
const PushDirection = Store.PushDirection;

const encoding = @import("encoding.zig");
const BulkArray = encoding.BulkArray;
const BulkString = encoding.BulkString;

const OK = "+OK\r\n";
const NULL = "$-1\r\n";
const PONG = "+PONG\r\n";
const ERROR_WRONGTYPE = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

const ParseError = error{
    InvalidArrayFormat,
    InvalidLength,
    MissingData,
    UnknownCommand,
};

const CommandName = enum {
    PING,
    ECHO,
    SET,
    GET,
    RPUSH,
    LPUSH,
    LRANGE,
    LLEN,
    LPOP,
};

const Command = union(CommandName) {
    PING: PING,
    ECHO: ECHO,
    SET: SET,
    GET: GET,
    RPUSH: PUSH,
    LPUSH: PUSH,
    LRANGE: LRANGE,
    LLEN: LLEN,
    LPOP: LPOP,

    pub fn do(
        command: Command,
        alloc: std.mem.Allocator,
        copy_capacity: usize,
        kv: *Store,
        out: *Writer,
    ) !void {
        switch (command) {
            .PING => |ping| return ping.do(out),
            .ECHO => |echo| return echo.do(out),
            .SET => |set| return set.do(out, kv),
            .GET => |get| return get.do(out, kv),
            .RPUSH, .LPUSH => |push| return push.do(out, kv),
            .LRANGE => |lrange| return lrange.do(alloc, copy_capacity, out, kv),
            .LLEN => |llen| return llen.do(out, kv),
            .LPOP => |lpop| return lpop.do(alloc, copy_capacity, out, kv),
        }
    }
};

pub const Runner = struct {
    fba: std.heap.FixedBufferAllocator,
    kv: *Store,

    parse_cap: usize,
    copy_cap: usize,

    pub fn init(gpa: std.mem.Allocator, config: Config, kv: *Store) !Runner {
        const L = config.list_length_max;
        const V = config.val_size_max;

        // ArrayList([]const u8) of largest possible command
        const parse_cap = (1 + 1 + L);
        const parse_size: u64 = (parse_cap * @sizeOf([]const u8));

        // ArrayList([]const u8) pointing to duplicated values
        const copy_cap = L;
        const copy_size = (L * @sizeOf([]const u8));
        const duplicated_size = (L * V);

        const fba_size: u64 = parse_size + copy_size + duplicated_size;
        const buffer = try gpa.alloc(u8, fba_size);
        const fba = std.heap.FixedBufferAllocator.init(buffer);

        return .{
            .fba = fba,
            .kv = kv,
            .parse_cap = parse_cap,
            .copy_cap = copy_cap,
        };
    }

    pub fn deinit(runner: *Runner, gpa: std.mem.Allocator) void {
        gpa.free(runner.fba.buffer);
    }

    pub fn run(runner: *Runner, request: []const u8, out: *Writer) !void {
        assert(runner.fba.end_index == 0);
        defer runner.fba.reset();

        const alloc = runner.fba.allocator();

        // TODO(nickmonad) handle parsing and exec errors
        var command = try parse(alloc, runner.parse_cap, request);
        try command.do(alloc, runner.copy_cap, runner.kv, out);
    }
};

// NOTE: Assume buffer holds a single command in RESP array format.
// Pipelining is technically a thing, but I'm not sure what the format should be...
// It seems like it should be the "inline command" format (not RESP): https://redis.io/docs/latest/develop/reference/protocol-spec/#inline-commands
pub fn parse(alloc: std.mem.Allocator, capacity: usize, buf: []const u8) !Command {
    var iter = try ParseIterator.init(alloc, capacity, buf);

    // command (first element)
    const cmd_parsed = c: {
        if (iter.next()) |cmd| {
            break :c cmd;
        }

        return ParseError.UnknownCommand;
    };

    const cmd = std.meta.stringToEnum(CommandName, cmd_parsed) orelse {
        return ParseError.UnknownCommand;
    };

    switch (cmd) {
        .PING => return Command{ .PING = PING{} },
        .ECHO => return Command{ .ECHO = try ECHO.parse(&iter) },
        .SET => return Command{ .SET = try SET.parse(&iter) },
        .GET => return Command{ .GET = try GET.parse(&iter) },
        .RPUSH => return Command{ .RPUSH = try PUSH.parse(&iter, .right) },
        .LPUSH => return Command{ .LPUSH = try PUSH.parse(&iter, .left) },
        .LRANGE => return Command{ .LRANGE = try LRANGE.parse(&iter) },
        .LLEN => return Command{ .LLEN = try LLEN.parse(&iter) },
        .LPOP => return Command{ .LPOP = try LPOP.parse(&iter) },
    }
}

const ParseIterator = struct {
    parsed: std.ArrayList([]const u8),
    index: usize = 0,

    fn init(alloc: std.mem.Allocator, capacity: usize, buf: []const u8) !ParseIterator {
        var parsed: std.ArrayList([]const u8) = try .initCapacity(alloc, capacity);
        errdefer parsed.deinit(alloc);

        // iterator over raw bulk array buffer
        var iter = std.mem.splitSequence(u8, buf, "\r\n");

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
            try parsed.appendBounded(data);
        }

        if (parsed.items.len != n) {
            return ParseError.InvalidArrayFormat;
        }

        return .{
            .parsed = parsed,
            .index = 0,
        };
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

    fn rest(iter: *ParseIterator) []const []const u8 {
        const start = iter.index;
        const end = iter.parsed.items.len;

        return iter.parsed.items[start..end];
    }
};

const PING = struct {
    fn do(_: PING, out: *Writer) !void {
        return out.print(PONG, .{});
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
        // TODO(nickmonad) Expiry currently has no effect
        // in the store. Need to re-add that in with an LRU eviction policy.
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

    fn do(cmd: GET, out: *Writer, kv: *Store) !void {
        const value = kv.get(cmd.key) orelse return out.print(NULL, .{});
        const inner = value.inner;

        switch (inner) {
            .string => |s| return BulkString.encode(out, s.data.slice()),
            else => return out.print(ERROR_WRONGTYPE, .{}),
        }
    }
};

const PUSH = struct {
    list: []const u8,
    elements: []const []const u8,
    direction: PushDirection,

    fn parse(
        iter: *ParseIterator,
        direction: PushDirection,
    ) !PUSH {
        const list = iter.next() orelse return ParseError.MissingData;
        const elements = iter.rest();

        return .{
            .list = list,
            .elements = elements,
            .direction = direction,
        };
    }

    fn do(cmd: PUSH, out: *Writer, kv: *Store) !void {
        const length = length: {
            var len: usize = undefined;
            for (cmd.elements) |element| {
                // TODO(nickmonad): batched kv.push
                // will avoid having to lookup the key on every iteration
                len = try kv.push(cmd.direction, cmd.list, element);
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
        copy_capacity: usize,
        out: *Writer,
        kv: *Store,
    ) !void {
        var copied: std.ArrayList([]const u8) = try .initCapacity(alloc, copy_capacity);
        defer copied.deinit(alloc);

        try kv.range(alloc, &copied, cmd.list, cmd.start, cmd.stop);
        return BulkArray.encode(out, copied.items);
    }
};

const LLEN = struct {
    list: []const u8,

    fn parse(iter: *ParseIterator) !LLEN {
        const list = iter.next() orelse return ParseError.MissingData;
        return .{ .list = list };
    }

    fn do(cmd: LLEN, out: *Writer, kv: *Store) !void {
        const value = kv.get(cmd.list);
        if (value) |v| {
            if (!v.inner.is_list()) {
                return out.print(ERROR_WRONGTYPE, .{});
            }

            const list = v.inner.list;
            return out.print(":{d}\r\n", .{list.len});
        }

        // no value exists under key, interpret as empty list
        return out.print(":0\r\n", .{});
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
        copy_capacity: usize,
        out: *Writer,
        kv: *Store,
    ) !void {
        var copied: std.ArrayList([]const u8) = try .initCapacity(alloc, copy_capacity);
        defer copied.deinit(alloc);

        try kv.pop(alloc, &copied, cmd.key, cmd.count);

        if (copied.items.len == 0) {
            return BulkString.encode(out, "");
        }

        if (copied.items.len == 1) {
            // only 1 element, return as bulk string
            const element = copied.items[0];
            return BulkString.encode(out, element);
        }

        // multiple elements
        return BulkArray.encode(out, copied.items);
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
    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.PING);
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
    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.ECHO);
    try std.testing.expectEqualSlices(u8, "hello, zig", parsed.ECHO.arg);
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

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.SET);
    try std.testing.expectEqualSlices(u8, "test", parsed.SET.key);
    try std.testing.expectEqualSlices(u8, "zig", parsed.SET.value);
}

test "parse SET key value PX expiry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "SET", "test", "zig", "PX", "100" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.SET);
    try std.testing.expectEqualSlices(u8, "test", parsed.SET.key);
    try std.testing.expectEqualSlices(u8, "zig", parsed.SET.value);
    try std.testing.expectEqual(100, parsed.SET.expires_in.?);
}

test "parse GET" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "GET", "test" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.GET);
    try std.testing.expectEqualSlices(u8, "test", parsed.GET.key);
}

test "parse RPUSH, 1 element" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "RPUSH", "test", "zig" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.RPUSH);
    try std.testing.expectEqualSlices(u8, "test", parsed.RPUSH.list);

    try std.testing.expectEqual(1, parsed.RPUSH.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", parsed.RPUSH.elements.items[0]);
}

test "parse RPUSH, mulitple elements" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "RPUSH", "test", "zig", "cool" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.RPUSH);
    try std.testing.expectEqualSlices(u8, "test", parsed.RPUSH.list);

    try std.testing.expectEqual(2, parsed.RPUSH.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", parsed.RPUSH.elements.items[0]);
    try std.testing.expectEqualSlices(u8, "cool", parsed.RPUSH.elements.items[1]);
}

test "parse LPUSH, 1 element" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LPUSH", "test", "zig" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.LPUSH);
    try std.testing.expectEqualSlices(u8, "test", parsed.LPUSH.list);

    try std.testing.expectEqual(1, parsed.LPUSH.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", parsed.LPUSH.elements.items[0]);
}

test "parse LRANGE" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LRANGE", "test", "0", "1" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.LRANGE);
    try std.testing.expectEqualSlices(u8, "test", parsed.LRANGE.list);
    try std.testing.expectEqual(0, parsed.LRANGE.start);
    try std.testing.expectEqual(1, parsed.LRANGE.stop);
}

test "parse LRANGE negative" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LRANGE", "test", "10", "-5" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.LRANGE);
    try std.testing.expectEqualSlices(u8, "test", parsed.LRANGE.list);
    try std.testing.expectEqual(10, parsed.LRANGE.start);
    try std.testing.expectEqual(-5, parsed.LRANGE.stop);
}

test "parse LLEN" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LLEN", "test" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.LLEN);
    try std.testing.expectEqualSlices(u8, "test", parsed.LLEN.list);
}

test "parse LPOP" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    var buf: [100]u8 = undefined;
    var w: Writer = .fixed(&buf);
    BA(&w, &.{ "LPOP", "test", "10" });

    const parsed = try parse(alloc, w.buffered());

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.LPOP);
    try std.testing.expectEqualSlices(u8, "test", parsed.LPOP.key);
    try std.testing.expectEqual(10, parsed.LPOP.count);
}
