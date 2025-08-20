const std = @import("std");
const assert = std.debug.assert;

pub const Key = []const u8;

pub const Value = union(enum) {
    string: String,
    list: List,
};

pub const String = struct {
    value: []const u8,
    expires_at: ?i64, // type defined by std.time
};

pub const List = struct {
    pub const Node = std.DoublyLinkedList(String).Node;
    value: std.DoublyLinkedList(String),
};

const PushDirection = enum { left, right };

pub const AllocatedList = struct {
    alloc: std.mem.Allocator,
    list: std.ArrayList(String),

    const Self = @This();

    fn init(alloc: std.mem.Allocator) Self {
        return .{ .alloc = alloc, .list = std.ArrayList(String).init(alloc) };
    }

    pub fn deinit(self: *Self) void {
        for (self.list.items) |item| {
            self.alloc.free(item.value);
        }

        self.list.deinit();
    }
};

pub const SetOptions = struct {
    expires_in: ?i64 = null, // milliseconds, type defined by std.time
};

pub const Store = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMap(Value),
    rw: std.Thread.RwLock,
    timer: *Timer,

    const Self = @This();

    pub fn init(alloc: std.mem.Allocator, timer: *Timer) Self {
        return .{
            .alloc = alloc,
            .map = std.StringHashMap(Value).init(alloc),
            .rw = .{},
            .timer = timer,
        };
    }

    // set
    // maps to key to value
    // key will be copied into internal space for later retrieval
    // value is copied by internal map and allocator
    // NOTE: for now, all keys are overwritten if they already exist
    pub fn set(self: *Store, key: Key, str: []const u8, opts: SetOptions) !void {
        self.rw.lock();
        defer self.rw.unlock();

        // copy the key and value before put...
        // we always have to copy key because keys are managed by us (see std.StringHashMap doc)
        // we also have to copy the value... otherwise we'd get an assignment-level copy of the value given
        // which, in this case, is a pointer to a slice, and we need the whole value
        const k = try self.alloc.dupe(u8, key);
        const s = try self.alloc.dupe(u8, str);

        // TODO: move this calculation to the higher Set command
        // then, we can take the String type as the argument to this `set()` function,
        // lining it up with the return of get()
        const expires_at = ex: {
            if (opts.expires_in) |e| {
                break :ex self.timer.getTime() + e;
            } else {
                break :ex null;
            }
        };

        const value: Value = .{ .string = .{ .value = s, .expires_at = expires_at } };
        return self.map.put(k, value);
    }

    // get
    // copies out the stored String using the given allocator
    // otherwise, we'd return pointers directly into the store's map, which another thread can manipulate
    // basically, once we're out of the lock guard (in the caller), we need to have safe memory access
    pub fn get(self: *Store, alloc: std.mem.Allocator, key: Key) !?String {
        self.rw.lockShared();
        defer self.rw.unlockShared();

        const value = self.map.get(key);
        if (value) |v| {
            assert(std.meta.activeTag(v) == .string);

            // check expires_at, return null if expired
            if (v.string.expires_at) |expires_at| {
                const now = self.timer.getTime();
                if (now > expires_at) return null;
                // TODO: also clear value from map?
            }

            // allocate copy
            const copied = try alloc.dupe(u8, v.string.value);
            return .{ .value = copied, .expires_at = v.string.expires_at };
        }

        return null;
    }

    // rpush
    // append element to (r)ight of list (i.e. the end)
    // return the current length of the list
    pub fn rpush(self: *Store, key: Key, element: []const u8) !usize {
        return self.push(key, element, .right);
    }

    // lpush
    // append element to (l)eft of list (i.e. the start)
    // return the current length of the list
    pub fn lpush(self: *Store, key: Key, element: []const u8) !usize {
        return self.push(key, element, .left);
    }

    // push
    // generalized list "push"
    // allows for .left or .right direction
    fn push(self: *Store, key: Key, element: []const u8, direction: PushDirection) !usize {
        self.rw.lock();
        defer self.rw.unlock();

        const exists = self.map.getPtr(key);
        if (exists) |list| {
            assert(std.meta.activeTag(list.*) == .list);
            // copy element
            const e = try self.alloc.dupe(u8, element);
            // append to existing list
            var node = try self.alloc.create(List.Node);
            node.data = .{ .value = e, .expires_at = null };

            switch (direction) {
                .left => list.*.list.value.prepend(node),
                .right => list.*.list.value.append(node),
            }

            return list.*.list.value.len;
        }

        // copy key and element... see note in `set()` for why
        const k = try self.alloc.dupe(u8, key);
        const e = try self.alloc.dupe(u8, element);

        // list does not exist.
        // create node
        var node = try self.alloc.create(List.Node);
        node.data = .{ .value = e, .expires_at = null };

        // and list
        var list = std.DoublyLinkedList(String){};
        list.append(node);

        const value: Value = .{ .list = .{ .value = list } };
        try self.map.put(k, value);

        return 1;
    }

    // lrange
    // copies the list (if exists) out using the given allocator
    // this prevents returning pointers directly into the map
    // (similar to get)
    pub fn lrange(self: *Store, alloc: std.mem.Allocator, key: Key, start: isize, stop: isize) !AllocatedList {
        self.rw.lockShared();
        defer self.rw.unlockShared();

        var copied = AllocatedList.init(alloc);
        errdefer copied.deinit();

        if (start > 0 and stop > 0 and start > stop) {
            // cannot index, return empty
            return copied;
        }

        const exists = self.map.get(key);
        if (exists) |value| {
            assert(std.meta.activeTag(value) == .list);
            const length = value.list.value.len;

            const i_start: usize = start: {
                if (start >= length) {
                    // cannot index, return empty
                    return copied;
                }

                if (start < 0) {
                    if (@abs(start) >= length) {
                        // trying to subtract past the length of the list
                        // force to 0
                        break :start 0;
                    }

                    break :start (length - @abs(start));
                }

                break :start @as(usize, @abs(start));
            };

            const i_stop: usize = stop: {
                // if stop is >= length of array, don't index past the array length
                if (stop >= length) {
                    break :stop length;
                }

                if (stop < 0) {
                    if (@abs(stop) >= length) {
                        // trying to subtract past the length of the list
                        // force to 0
                        break :stop 0;
                    }

                    break :stop (length - @abs(stop));
                }

                break :stop @as(usize, @abs(stop));
            };

            const list = value.list.value;
            var current = list.first;
            var i: usize = 0;
            while (current) |node| {
                if (i_start <= i and i <= i_stop) {
                    const str = try alloc.dupe(u8, node.data.value);
                    try copied.list.append(.{ .value = str, .expires_at = null });
                }

                current = node.next;
                i += 1;
            }
        }

        return copied;
    }

    // llen
    pub fn llen(self: *Store, key: Key) usize {
        self.rw.lockShared();
        defer self.rw.unlockShared();

        const exists = self.map.get(key);
        if (exists) |value| {
            assert(std.meta.activeTag(value) == .list);
            return value.list.value.len;
        }

        return 0;
    }

    // lpop
    pub fn lpop(self: *Store, alloc: std.mem.Allocator, key: Key, count: usize) !?AllocatedList {
        self.rw.lock();
        defer self.rw.unlock();

        const exists = self.map.getPtr(key);
        if (exists) |value| {
            assert(std.meta.activeTag(value.*) == .list);

            // prepare to copy elements out of list
            var copied = AllocatedList.init(alloc);
            errdefer copied.deinit();

            for (0..count) |_| {
                if (value.*.list.value.popFirst()) |node| {
                    const str = try alloc.dupe(u8, node.data.value);
                    try copied.list.append(.{ .value = str, .expires_at = null });

                    self.alloc.free(node.data.value);
                    self.alloc.destroy(node);
                }
            }

            return copied;
        }

        return null;
    }

    pub fn deinit(self: *Store) void {
        self.rw.lock();
        defer self.rw.unlock();

        var iter = self.map.iterator();
        while (iter.next()) |entry| {
            // free all keys and values
            self.alloc.free(entry.key_ptr.*);

            const v = entry.value_ptr.*;
            switch (std.meta.activeTag(v)) {
                .string => self.alloc.free(v.string.value),
                .list => {
                    var node = v.list.value.first;
                    while (node) |n| {
                        // free inner string array
                        self.alloc.free(n.data.value);
                        node = n.next;

                        // destory node itself
                        self.alloc.destroy(n);
                    }
                },
            }
        }

        self.map.deinit();
    }
};

pub const TimerType = enum {
    system,
    mock,
};

pub const Timer = union(TimerType) {
    system: *SystemTimer,
    mock: *MockTimer,

    pub fn getTime(self: *Timer) i64 {
        switch (self.*) {
            .system => |system| return system.getTime(),
            .mock => |mock| return mock.getTime(),
        }
    }
};

pub const SystemTimer = struct {
    pub fn getTime(_: *SystemTimer) i64 {
        return std.time.milliTimestamp();
    }
};

pub const MockTimer = struct {
    current: i64 = 0,

    pub fn getTime(self: *MockTimer) i64 {
        return self.current;
    }
};

test "basic set and get" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var time = MockTimer{};
    var timer = Timer{ .mock = &time };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    // simple, no expire
    try store.set("a", "zig", .{});
    const a = try store.get(alloc, "a");
    defer alloc.free(a.?.value);
    try testing.expectEqualDeep(
        String{ .value = "zig", .expires_at = null },
        a.?,
    );

    // expire
    try store.set("b", "expires", .{ .expires_in = 100 });
    const b = try store.get(alloc, "b");
    defer alloc.free(b.?.value);

    try testing.expectEqualDeep(
        String{ .value = "expires", .expires_at = 100 },
        b.?,
    );

    // ...advance time
    time.current = 200;
    try testing.expectEqualDeep(
        null,
        try store.get(alloc, "b"),
    );
}

test "rpush new list, 1 element" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    try std.testing.expectEqual(1, try store.rpush("new", "zig"));
    try std.testing.expectEqual(1, try store.rpush("another", "zig"));
}

test "rpush existing list, 1 element" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    // create existing list
    try std.testing.expectEqual(1, try store.rpush("new", "zig"));

    // append to list
    try std.testing.expectEqual(2, try store.rpush("new", "test"));
}

test "lpush new list, 1 element" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    try std.testing.expectEqual(1, try store.lpush("new", "zig"));
    try std.testing.expectEqual(1, try store.lpush("another", "zig"));
}

test "lpush existing list, 1 element" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    // create existing list
    try std.testing.expectEqual(1, try store.lpush("new", "zig"));

    // append to list
    try std.testing.expectEqual(2, try store.lpush("new", "test"));
}

test "lrange (rpush) list does not exist" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    const expected = AllocatedList.init(alloc);
    const actual = try store.lrange(alloc, "nope", 0, 10);

    try std.testing.expectEqual(expected, actual);
}

test "lrange (rpush) list exists" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    _ = try store.rpush("list", "a");
    _ = try store.rpush("list", "b");
    _ = try store.rpush("list", "c");

    var expected = AllocatedList.init(alloc);
    // NOTE: deinit the underlying list directly in tests
    // If we don't, we segfault trying to dealloc the `.value` items below
    // (since they are statically initialized)
    defer expected.list.deinit();

    try expected.list.append(.{ .value = "a", .expires_at = null });
    try expected.list.append(.{ .value = "b", .expires_at = null });
    try expected.list.append(.{ .value = "c", .expires_at = null });

    var inrange = try store.lrange(alloc, "list", 0, 2);
    var outrange = try store.lrange(alloc, "list", 0, 3);
    var wayoutrange = try store.lrange(alloc, "list", 0, 100);
    // NOTE: These are safe to deinit/dealloc, as they are dynamically allocated in lrange()
    defer inrange.deinit();
    defer outrange.deinit();
    defer wayoutrange.deinit();

    try std.testing.expectEqualDeep(expected, inrange);
    try std.testing.expectEqualDeep(expected, outrange);
    try std.testing.expectEqualDeep(expected, wayoutrange);
}

test "lrange (rpush) weird start and stop" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    var expected = AllocatedList.init(alloc);
    defer expected.list.deinit();

    // start > stop
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", 1, 0));
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", 100, 99));

    // start == stop, empty
    var empty = try store.lrange(alloc, "list", 0, 0);
    defer empty.deinit();
    try std.testing.expectEqualDeep(expected, empty);

    // start == stop, multiple elements
    _ = try store.rpush("list", "a");
    _ = try store.rpush("list", "b");
    _ = try store.rpush("list", "c");

    try expected.list.append(.{ .value = "a", .expires_at = null });
    var one_element = try store.lrange(alloc, "list", 0, 0);
    defer one_element.deinit();

    try std.testing.expectEqualDeep(expected, one_element);
}

test "lrange (rpush) negative start and stop" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    var expected = AllocatedList.init(alloc);
    defer expected.list.deinit();

    // negative, empty
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", -10, -10));
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", -10, -5));
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", -10, 0));
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", 0, -3));

    // ... add list elements
    _ = try store.rpush("list", "a");
    _ = try store.rpush("list", "b");
    _ = try store.rpush("list", "c");
    _ = try store.rpush("list", "d");

    try expected.list.append(.{ .value = "a", .expires_at = null });
    try expected.list.append(.{ .value = "b", .expires_at = null });
    try expected.list.append(.{ .value = "c", .expires_at = null });
    try expected.list.append(.{ .value = "d", .expires_at = null });

    var test1 = try store.lrange(alloc, "list", -1, -1);
    defer test1.deinit();
    try std.testing.expectEqualDeep(expected.list.items[3..4], test1.list.items);

    var test2 = try store.lrange(alloc, "list", -2, -1);
    defer test2.deinit();
    try std.testing.expectEqualDeep(expected.list.items[2..4], test2.list.items);

    var test3 = try store.lrange(alloc, "list", 0, -3);
    defer test3.deinit();
    try std.testing.expectEqualDeep(expected.list.items[0..2], test3.list.items);

    var test4 = try store.lrange(alloc, "list", 0, -1);
    defer test4.deinit();
    try std.testing.expectEqualDeep(expected.list.items[0..4], test4.list.items);
}

test "lrange (lpush)" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    var expected = AllocatedList.init(alloc);
    defer expected.list.deinit();

    // negative, empty
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", -10, -10));
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", -10, -5));
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", -10, 0));
    try std.testing.expectEqualDeep(expected, try store.lrange(alloc, "list", 0, -3));

    // ... add list elements
    _ = try store.lpush("list", "a");
    _ = try store.lpush("list", "b");
    _ = try store.lpush("list", "c");
    _ = try store.lpush("list", "d");

    try expected.list.append(.{ .value = "d", .expires_at = null });
    try expected.list.append(.{ .value = "c", .expires_at = null });
    try expected.list.append(.{ .value = "b", .expires_at = null });
    try expected.list.append(.{ .value = "a", .expires_at = null });

    var test1 = try store.lrange(alloc, "list", -1, -1);
    defer test1.deinit();
    try std.testing.expectEqualDeep(expected.list.items[3..4], test1.list.items);

    var test2 = try store.lrange(alloc, "list", -2, -1);
    defer test2.deinit();
    try std.testing.expectEqualDeep(expected.list.items[2..4], test2.list.items);

    var test3 = try store.lrange(alloc, "list", 0, -3);
    defer test3.deinit();
    try std.testing.expectEqualDeep(expected.list.items[0..2], test3.list.items);

    var test4 = try store.lrange(alloc, "list", 0, -1);
    defer test4.deinit();
    try std.testing.expectEqualDeep(expected.list.items[0..4], test4.list.items);
}

test "llen" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    // empty list
    try std.testing.expectEqual(0, store.llen("list"));

    // ... add list elements
    _ = try store.lpush("list", "a");
    try std.testing.expectEqual(1, store.llen("list"));

    _ = try store.lpush("list", "a");
    _ = try store.lpush("list", "a");
    try std.testing.expectEqual(3, store.llen("list"));

    _ = try store.rpush("list", "a");
    _ = try store.rpush("list", "a");
    try std.testing.expectEqual(5, store.llen("list"));
}

test "lpop" {
    const testing = std.testing;
    const alloc = testing.allocator;
    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = Store.init(alloc, &timer);
    defer store.deinit();

    _ = try store.rpush("list", "a");
    _ = try store.rpush("list", "b");
    _ = try store.rpush("list", "c");
    _ = try store.rpush("list", "d");

    var expected = AllocatedList.init(alloc);
    defer expected.list.deinit();

    // count = 0, return empty
    var empty = try store.lpop(alloc, "list", 0);
    defer empty.?.deinit();
    try std.testing.expectEqualDeep(expected, empty);

    // count = 1
    try expected.list.append(.{ .value = "a", .expires_at = null });
    var count1 = try store.lpop(alloc, "list", 1);
    defer count1.?.deinit();

    try std.testing.expectEqualDeep(expected, count1.?);
    try std.testing.expectEqual(3, store.llen("list"));

    // count = 2
    expected.list.clearAndFree();
    try expected.list.append(.{ .value = "b", .expires_at = null });
    try expected.list.append(.{ .value = "c", .expires_at = null });
    var count2 = try store.lpop(alloc, "list", 2);
    defer count2.?.deinit();

    try std.testing.expectEqualDeep(expected, count2.?);
}
