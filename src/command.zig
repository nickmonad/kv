const std = @import("std");

const Store = @import("store.zig").Store;
const encoding = @import("encoding.zig");
const BulkArray = encoding.BulkArray;
const BulkString = encoding.BulkString;

const OK = "+OK\r\n";
const NULL = "$-1\r\n";

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
    const Self = @This();

    parsed: std.ArrayList([]const u8),
    index: usize = 0,

    fn init(alloc: std.mem.Allocator, buf: []const u8) !Self {
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

        return Self{ .parsed = parsed };
    }

    fn deinit(self: *Self, alloc: std.mem.Allocator) void {
        self.parsed.deinit(alloc);
    }

    fn next(self: *Self) ?[]const u8 {
        if (self.index >= self.parsed.items.len) {
            return null;
        }

        const current = self.index;
        self.index += 1;

        return self.parsed.items[current];
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

    const Self = @This();

    pub fn do(self: *Self, alloc: std.mem.Allocator, kv: *Store) !?[]const u8 {
        switch (self.*) {
            .ping => |ping| return ping.do(),
            .echo => |echo| return echo.do(alloc),
            .set => |set| return set.do(kv),
            .get => |get| return get.do(alloc, kv),
            .rpush, .lpush => |*push| {
                defer push.deinit(alloc);
                return push.do(alloc, kv);
            },
            .lrange => |lrange| return lrange.do(alloc, kv),
            .llen => |llen| return llen.do(alloc, kv),
            .lpop => |lpop| return lpop.do(alloc, kv),
        }
    }
};

const PING = struct {
    fn do(_: PING) []const u8 {
        return "+PONG\r\n";
    }
};

const ECHO = struct {
    arg: []const u8,
    const Self = @This();

    fn parse(iter: *ParseIterator) !Self {
        const arg = iter.next() orelse return ParseError.MissingData;
        return .{ .arg = arg };
    }

    fn do(self: Self, alloc: std.mem.Allocator) !?[]const u8 {
        const encoded = try BulkString.encode(alloc, self.arg);
        return encoded.str;
    }
};

const SET = struct {
    key: []const u8,
    value: []const u8,
    expires_in: ?i64 = null,

    const Self = @This();

    fn parse(iter: *ParseIterator) !Self {
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

    fn do(self: Self, kv: *Store) !?[]const u8 {
        try kv.set(self.key, self.value, .{ .expires_in = self.expires_in });
        return OK;
    }
};

const GET = struct {
    key: []const u8,

    const Self = @This();

    fn parse(iter: *ParseIterator) !Self {
        const key = iter.next() orelse return ParseError.MissingData;
        return .{ .key = key };
    }

    fn do(self: Self, alloc: std.mem.Allocator, kv: *Store) !?[]const u8 {
        const value = try kv.get(alloc, self.key);
        if (value) |v| {
            defer alloc.free(v.value);

            const bulk = try BulkString.encode(alloc, v.value);
            return bulk.str;
        }

        return NULL;
    }
};

const PUSH = struct {
    list: []const u8,
    elements: std.ArrayList([]const u8),
    direction: Direction,

    const Self = @This();
    const Direction = enum { left, right };

    fn parse(alloc: std.mem.Allocator, iter: *ParseIterator, direction: Direction) !Self {
        const list = iter.next() orelse return ParseError.MissingData;
        var elements: std.ArrayList([]const u8) = .empty;

        while (iter.next()) |element| {
            try elements.append(alloc, element);
        }

        return .{ .list = list, .elements = elements, .direction = direction };
    }

    fn do(self: Self, alloc: std.mem.Allocator, kv: *Store) !?[]const u8 {
        const length = length: {
            var len: usize = undefined;
            for (self.elements.items) |element| {
                // TODO: probably gonna be more efficient to add an kv method
                // for rpushing multiple elements, so we'd don't have to lookup the key every time
                // Technically, this has a race condition anyway. Another thread could
                // snake in a call to kv.rpush, invalidating the client's expections
                // of the returned length value
                len = len: switch (self.direction) {
                    .left => break :len try kv.lpush(self.list, element),
                    .right => break :len try kv.rpush(self.list, element),
                };
            }

            break :length len;
        };

        return try std.fmt.allocPrint(alloc, ":{d}\r\n", .{length}); // TODO: abstract this out to RESP Int type
    }

    fn deinit(self: *Self, alloc: std.mem.Allocator) void {
        self.elements.deinit(alloc);
    }
};

const LRANGE = struct {
    list: []const u8,
    start: isize,
    stop: isize,

    const Self = @This();

    fn parse(iter: *ParseIterator) !Self {
        const list = iter.next() orelse return ParseError.MissingData;
        const start = try std.fmt.parseInt(isize, iter.next() orelse return ParseError.MissingData, 10);
        const stop = try std.fmt.parseInt(isize, iter.next() orelse return ParseError.MissingData, 10);

        return .{
            .list = list,
            .start = start,
            .stop = stop,
        };
    }

    fn do(self: Self, alloc: std.mem.Allocator, kv: *Store) !?[]const u8 {
        var list = try kv.lrange(alloc, self.list, self.start, self.stop);
        defer list.deinit(alloc);

        var to_encode: std.ArrayList([]const u8) = .empty;
        defer to_encode.deinit(alloc);

        for (list.list.items) |item| {
            try to_encode.append(alloc, item.value);
        }

        const encoded = try BulkArray.encode(alloc, to_encode.items);
        return encoded.str;
    }
};

const LLEN = struct {
    list: []const u8,

    const Self = @This();

    fn parse(iter: *ParseIterator) !Self {
        const list = iter.next() orelse return ParseError.MissingData;
        return .{ .list = list };
    }

    fn do(self: Self, alloc: std.mem.Allocator, kv: *Store) !?[]const u8 {
        const length = kv.llen(self.list);
        return try std.fmt.allocPrint(alloc, ":{d}\r\n", .{length}); // TODO: abstract this out to RESP Int type
    }
};

const LPOP = struct {
    key: []const u8,
    count: usize = 1,

    const Self = @This();

    fn parse(iter: *ParseIterator) !Self {
        const key = iter.next() orelse return ParseError.MissingData;

        if (iter.next()) |count| {
            const c = try std.fmt.parseInt(usize, count, 10);
            return .{ .key = key, .count = c };
        }

        return .{ .key = key };
    }

    fn do(self: Self, alloc: std.mem.Allocator, kv: *Store) !?[]const u8 {
        var list = try kv.lpop(alloc, self.key, self.count) orelse return NULL;
        defer list.deinit(alloc);

        if (list.list.items.len == 0) {
            const encoded = try BulkString.encode(alloc, "");
            return encoded.str;
        }

        if (list.list.items.len == 1) {
            // only 1 element, return as bulk string
            const element = list.list.items[0];
            const encoded = try BulkString.encode(alloc, element.value);
            return encoded.str;
        }

        // multiple elements
        var to_encode: std.ArrayList([]const u8) = .empty;
        defer to_encode.deinit(alloc);

        for (list.list.items) |item| {
            try to_encode.append(alloc, item.value);
        }

        const encoded = try BulkArray.encode(alloc, to_encode.items);
        return encoded.str;
    }
};

// tests

// test helper
// create a BulkArray, assuming the std.testing.allocator (ignoring allocation errors)
// usage: BA(&.{ "ECHO", "test" })
// Don't forget to free the returned .str
fn BA(elements: []const []const u8) BulkArray {
    return BulkArray.encode(std.testing.allocator, elements) catch unreachable;
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

    for (0..100) |_| {
        // this thing had better be null!
        try std.testing.expectEqual(null, iter.next());
    }
}

test "ParseIterator multiple elements" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{ "SET", "test", "zig" });
    defer alloc.free(cmd.str);

    var iter = try ParseIterator.init(alloc, cmd.str);
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
    const alloc = std.testing.allocator;

    const cmd = BA(&.{"PING"});
    defer alloc.free(cmd.str);

    const parsed = try parse(alloc, cmd.str);
    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.ping);
}

test "parse ECHO error missing" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{"ECHO"});
    defer alloc.free(cmd.str);

    try std.testing.expectError(ParseError.MissingData, parse(alloc, cmd.str));
}

test "parse ECHO arg" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{ "ECHO", "hello, zig" });
    defer alloc.free(cmd.str);
    const parsed = try parse(alloc, cmd.str);

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.echo);
    try std.testing.expectEqualSlices(u8, "hello, zig", parsed.echo.arg);
}

test "parse SET error missing" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{ "SET", "test" });
    defer alloc.free(cmd.str);
    try std.testing.expectError(ParseError.MissingData, parse(alloc, cmd.str));
}

test "parse SET key value" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{ "SET", "test", "zig" });
    defer alloc.free(cmd.str);
    const parsed = try parse(alloc, cmd.str);

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.set);
    try std.testing.expectEqualSlices(u8, "test", parsed.set.key);
    try std.testing.expectEqualSlices(u8, "zig", parsed.set.value);
}

test "parse SET key value PX expiry" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{ "SET", "test", "zig", "PX", "100" });
    defer alloc.free(cmd.str);
    const parsed = try parse(alloc, cmd.str);

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.set);
    try std.testing.expectEqualSlices(u8, "test", parsed.set.key);
    try std.testing.expectEqualSlices(u8, "zig", parsed.set.value);
    try std.testing.expectEqual(100, parsed.set.expires_in.?);
}

test "parse GET" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{ "GET", "test" });
    defer alloc.free(cmd.str);
    const parsed = try parse(alloc, cmd.str);

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.get);
    try std.testing.expectEqualSlices(u8, "test", parsed.get.key);
}

test "parse RPUSH, 1 element" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{ "RPUSH", "test", "zig" });
    defer alloc.free(cmd.str);
    var parsed = try parse(alloc, cmd.str);
    defer parsed.rpush.deinit(alloc);

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.rpush);
    try std.testing.expectEqualSlices(u8, "test", parsed.rpush.list);

    try std.testing.expectEqual(1, parsed.rpush.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", parsed.rpush.elements.items[0]);
}

test "parse RPUSH, mulitple elements" {
    const alloc = std.testing.allocator;

    const ba = BA(&.{ "RPUSH", "test", "zig", "cool" });
    defer alloc.free(ba.str);
    var cmd = try parse(alloc, ba.str);
    defer cmd.rpush.deinit(alloc);

    try std.testing.expect(std.meta.activeTag(cmd) == CommandName.rpush);
    try std.testing.expectEqualSlices(u8, "test", cmd.rpush.list);

    try std.testing.expectEqual(2, cmd.rpush.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", cmd.rpush.elements.items[0]);
    try std.testing.expectEqualSlices(u8, "cool", cmd.rpush.elements.items[1]);
}

test "parse LPUSH, 1 element" {
    const alloc = std.testing.allocator;

    const cmd = BA(&.{ "LPUSH", "test", "zig" });
    defer alloc.free(cmd.str);
    var parsed = try parse(alloc, cmd.str);
    defer parsed.lpush.deinit(alloc);

    try std.testing.expect(std.meta.activeTag(parsed) == CommandName.lpush);
    try std.testing.expectEqualSlices(u8, "test", parsed.lpush.list);

    try std.testing.expectEqual(1, parsed.lpush.elements.items.len);
    try std.testing.expectEqualSlices(u8, "zig", parsed.lpush.elements.items[0]);
}

test "parse LRANGE" {
    const alloc = std.testing.allocator;

    const ba = BA(&.{ "LRANGE", "test", "0", "1" });
    defer alloc.free(ba.str);
    const cmd = try parse(alloc, ba.str);

    try std.testing.expect(std.meta.activeTag(cmd) == CommandName.lrange);
    try std.testing.expectEqualSlices(u8, "test", cmd.lrange.list);
    try std.testing.expectEqual(0, cmd.lrange.start);
    try std.testing.expectEqual(1, cmd.lrange.stop);
}

test "parse LRANGE negative" {
    const alloc = std.testing.allocator;

    const ba = BA(&.{ "LRANGE", "test", "10", "-5" });
    defer alloc.free(ba.str);
    const cmd = try parse(alloc, ba.str);

    try std.testing.expect(std.meta.activeTag(cmd) == CommandName.lrange);
    try std.testing.expectEqualSlices(u8, "test", cmd.lrange.list);
    try std.testing.expectEqual(10, cmd.lrange.start);
    try std.testing.expectEqual(-5, cmd.lrange.stop);
}

test "parse LLEN" {
    const alloc = std.testing.allocator;

    const ba = BA(&.{ "LLEN", "test" });
    defer alloc.free(ba.str);
    const cmd = try parse(alloc, ba.str);

    try std.testing.expect(std.meta.activeTag(cmd) == CommandName.llen);
    try std.testing.expectEqualSlices(u8, "test", cmd.llen.list);
}

test "parse LPOP" {
    const alloc = std.testing.allocator;

    const ba = BA(&.{ "LPOP", "test" });
    defer alloc.free(ba.str);
    const cmd = try parse(alloc, ba.str);

    try std.testing.expect(std.meta.activeTag(cmd) == CommandName.lpop);
    try std.testing.expectEqualSlices(u8, "test", cmd.lpop.key);

    const ba_count = BA(&.{ "LPOP", "test", "10" });
    defer alloc.free(ba_count.str);
    const cmd_count = try parse(alloc, ba_count.str);

    try std.testing.expect(std.meta.activeTag(cmd_count) == CommandName.lpop);
    try std.testing.expectEqualSlices(u8, "test", cmd_count.lpop.key);
    try std.testing.expectEqual(10, cmd_count.lpop.count);
}

test "parse fuzz" {
    const Context = struct {
        fn testOne(context: @This(), input: []const u8) !void {
            _ = context;
            try std.testing.expect(std.meta.isError(parse(std.testing.allocator, input)));
        }
    };

    try std.testing.fuzz(Context{}, Context.testOne, .{ .corpus = &.{ "SET", "GET" } });
}
