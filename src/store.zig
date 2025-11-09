const std = @import("std");
const assert = std.debug.assert;

const buffer_pool = @import("./buffer_pool.zig");
const Buffer = buffer_pool.Buffer;
const BufferPool = buffer_pool.BufferPool;

const Entry = struct {
    // associate value with key
    // so that we can free it as needed
    key: *const Buffer,
    value: Value,
};

const Value = union(enum) {
    string: String,
    list: List,
};

const String = struct {
    data: *const Buffer,
};

const List = struct {
    linked: std.DoublyLinkedList,
    len: usize = 0,
};

const ListItem = struct {
    node: std.DoublyLinkedList.Node,
    data: String,
};

pub const PushDirection = enum { left, right };

pub const SetOptions = struct {
    expires_in: ?i64 = null, // milliseconds, type defined by std.time
};

pub const Store = struct {
    map: std.StringHashMapUnmanaged(Entry),

    keys: BufferPool,
    values: BufferPool,

    timer: *Timer,

    /// Initialize the Store with a given number of keys.
    /// Space for keys will be allocated, along with space of values.
    /// Currently, the space allocated for values will be double the amount allocated for keys.
    /// This is to ensure that the number of _associations_ of key to value can be closer
    /// to what is configured here as `size`, in the event a handful of keys allocate many
    /// values as part of a list.
    pub fn init(gpa: std.mem.Allocator, size: u32, timer: *Timer) error{OutOfMemory}!Store {
        const num_keys = size;
        const num_vals = num_keys * 2;

        var map: std.StringHashMapUnmanaged(Entry) = .empty;
        try map.ensureTotalCapacity(gpa, num_keys);

        // TODO(nickmonad) config
        const keys = try BufferPool.init(gpa, num_keys, 1024);
        const values = try BufferPool.init(gpa, num_vals, 1024);

        return .{
            .map = map,
            .keys = keys,
            .values = values,
            .timer = timer,
        };
    }

    pub fn deinit(store: *Store, gpa: std.mem.Allocator) void {
        store.keys.deinit();
        store.values.deinit();

        store.map.deinit(gpa);
    }

    // debug stats
    pub fn debug(store: *Store) void {
        std.debug.print("map capacity = {d}, map size = {d}, available = {d}\n", .{
            store.map.capacity(),
            store.map.size,
            store.map.available,
        });

        var it = store.map.iterator();
        while (it.next()) |entry| {
            std.debug.print("{s}\n", .{entry.key_ptr.*});
        }
    }

    pub fn set(store: *Store, key_data: []const u8, val_data: []const u8, _: SetOptions) error{OutOfMemory}!void {
        if (store.map.available == 0) {
            // while the map _technically_ has capacity, we can't associate
            // any more keys beyond the configured load factor, as we risk
            // a full scan on a hash conflict
            return error.OutOfMemory;
        }

        // TODO(nickmonad)
        // check if key or value length is greater than configured maximums

        // always remove any conflicting key/value
        _ = store.map.remove(key_data);

        const key = try store.keys.reserve();
        const val = try store.values.reserve();

        try key.write(key_data);
        try val.write(val_data);

        // NOTE(nickmonad)
        // It is CRITICAL that we use key.slice() here, instead of key_data.
        // Using key_data would result in inconsistent GET results, due to
        // how the connection pool buffers interact with this function call.
        const value: Value = .{ .string = .{ .data = val } };
        store.map.putAssumeCapacity(key.slice(), .{ .key = key, .value = value });
    }

    pub fn get(store: *Store, key: []const u8) ?[]const u8 {
        const entry: Entry = store.map.get(key) orelse return null;
        const value = entry.value;

        // TODO(nickmond) this asserts the value stored at key is a string
        // we need to return an error if it's a list
        // or... we handle all that at the command "protocol" level and just
        // faithfully return values stored in this map
        return value.string.data.slice();
    }

    pub fn remove(store: *Store, key: []const u8) bool {
        const kv = store.map.fetchRemove(key) orelse return false;

        // TODO(nickmonad) handle removal of lists as well

        store.keys.release(@constCast(kv.value.key));
        store.values.release(@constCast(kv.value.value.string.data));

        return true;
    }
};

pub const TimerType = enum {
    system,
    mock,
};

pub const Timer = union(TimerType) {
    system: *SystemTimer,
    mock: *MockTimer,

    pub fn getTime(timer: *Timer) i64 {
        switch (timer.*) {
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

    pub fn getTime(mock: *MockTimer) i64 {
        return mock.current;
    }
};

test "basic usage" {
    const alloc = std.testing.allocator;

    var mock = MockTimer{};
    var timer = Timer{ .mock = &mock };

    var store = try Store.init(alloc, 1, &timer);
    defer store.deinit(alloc);

    try store.set("zig", "test", .{});
    const value = store.get("zig").?;

    try std.testing.expectEqualSlices(u8, "test", value);
    try std.testing.expect(store.remove("zig"));
}
