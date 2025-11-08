const std = @import("std");

const buffer_pool = @import("./buffer_pool.zig");
const Buffer = buffer_pool.Buffer;
const BufferPool = buffer_pool.BufferPool;

const Value = struct {
    key: *const Buffer,
    buffer: *const Buffer,
};

pub const Store = struct {
    map: std.StringHashMapUnmanaged(Value),

    keys: BufferPool,
    values: BufferPool,

    /// Initialize the Store with a given number of keys.
    /// Space for keys will be allocated, along with space of values.
    /// Currently, the space allocated for values will be double the amount allocated for keys.
    /// This is to ensure that the number of _associations_ of key to value can be closer
    /// to what is configured here as `size`, in the event a handful of keys allocate many
    /// values as part of a list.
    pub fn init(gpa: std.mem.Allocator, size: u32) error{OutOfMemory}!Store {
        const num_keys = size;
        const num_vals = num_keys * 2;

        var map: std.StringHashMapUnmanaged(Value) = .empty;
        try map.ensureTotalCapacity(gpa, num_keys);

        // TODO(nickmonad) config
        const keys = try BufferPool.init(gpa, num_keys, 1024);
        const values = try BufferPool.init(gpa, num_vals, 1024);

        return .{
            .map = map,
            .keys = keys,
            .values = values,
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
    }

    pub fn set(store: *Store, key_data: []const u8, val_data: []const u8) error{OutOfMemory}!void {
        if (store.map.available == 0) {
            // while the map _technically_ has capacity, we can't associate
            // any more keys beyond the configured load factor, as we risk
            // a full scan on a hash conflict
            return error.OutOfMemory;
        }

        // TODO(nickmonad)
        // check if key or value length is greater than configured maximums

        const key = try store.keys.reserve();
        const val = try store.values.reserve();

        try key.write(key_data);
        try val.write(val_data);

        store.map.putAssumeCapacity(key.slice(), .{ .key = key, .buffer = val });
    }

    pub fn get(store: *Store, key: []const u8) ?[]const u8 {
        const val: Value = store.map.get(key) orelse return null;
        return val.buffer.slice();
    }

    pub fn remove(store: *Store, key: []const u8) bool {
        const kv = store.map.fetchRemove(key) orelse return false;

        store.keys.release(@constCast(kv.value.key));
        store.values.release(@constCast(kv.value.buffer));

        return true;
    }
};

test "basic usage" {
    const alloc = std.testing.allocator;

    var store = try Store.init(alloc, 10);
    defer store.deinit(alloc);

    try store.set("zig", "test");
    const value = store.get("zig").?;

    try std.testing.expectEqualSlices(u8, "test", value);
    try std.testing.expect(store.remove("zig"));
}
