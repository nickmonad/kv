const std = @import("std");
const assert = std.debug.assert;

const buffer_pool = @import("./buffer_pool.zig");
const Buffer = buffer_pool.Buffer;
const BufferPool = buffer_pool.BufferPool;

/// Direct value type referenced by the internal hash map.
/// As far as lookups go, this is all the hash map cares about storing.
/// We call this "Value" so it aligns well with the attributes of
/// structures returned by the hash map. (i.e. "value_ptr", etc)
pub const Value = struct {
    // Store a reference to the key, so it can be free'd when
    // this value is removed from the map.
    key: *const Buffer,
    inner: InnerValue,
};

/// This is the "logical" value our application cares about.
/// Each key can refer to either a standalone "string" or a list of those.
pub const InnerValue = union(enum) {
    string: String,
    list: List,

    pub fn is_string(v: InnerValue) bool {
        return std.meta.activeTag(v) == .string;
    }

    pub fn is_list(v: InnerValue) bool {
        return std.meta.activeTag(v) == .list;
    }
};

pub const String = struct {
    data: *const Buffer,
};

pub const List = struct {
    linked: std.DoublyLinkedList,
    len: usize = 0,
};

pub const ListItem = struct {
    node: std.DoublyLinkedList.Node,
    string: String,
};

pub const SetOptions = struct {
    expires_in: ?i64 = null, // milliseconds, type defined by std.time
};

pub const AllocatedList = struct {
    list: std.ArrayList([]const u8),

    fn init() AllocatedList {
        return .{ .list = .empty };
    }

    pub fn deinit(list: *AllocatedList, alloc: std.mem.Allocator) void {
        for (list.list.items) |item| {
            alloc.free(item);
        }

        list.list.deinit(alloc);
    }
};

pub const Store = struct {
    pub const PushDirection = enum { left, right };

    const ListItemPool = std.heap.MemoryPoolExtra(ListItem, .{ .growable = false });

    map: std.StringHashMapUnmanaged(Value),

    keys: BufferPool,
    values: BufferPool,

    list_items: ListItemPool,

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

        // Create a pool of list items for list values.
        // In practice, we may get really poor utilization of this allocated space,
        // if our storage is heavy on single string values. Ideally, our static allocation
        // could share more space with other data types, but we aren't smart enough for that yet.
        const list_items = try ListItemPool.initPreheated(gpa, num_vals);

        return .{
            .map = map,
            .keys = keys,
            .values = values,
            .list_items = list_items,
        };
    }

    pub fn deinit(store: *Store, gpa: std.mem.Allocator) void {
        store.list_items.deinit();
        store.values.deinit();
        store.keys.deinit();

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

    fn has_availability(store: *Store) error{OutOfMemory}!void {
        if (store.map.available == 0) {
            // while the map _technically_ has capacity, we can't associate
            // any more keys beyond the configured load factor, as we risk
            // a full scan on a hash conflict
            return error.OutOfMemory;
        }
    }

    pub fn set(store: *Store, key_data: []const u8, val_data: []const u8, _: SetOptions) error{OutOfMemory}!void {
        try store.has_availability();

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
        const inner: InnerValue = .{ .string = .{ .data = val } };
        store.map.putAssumeCapacity(key.slice(), .{ .key = key, .inner = inner });
    }

    pub fn get(store: *Store, key: []const u8) ?Value {
        return store.map.get(key);
    }

    pub fn push(store: *Store, direction: PushDirection, key_data: []const u8, element: []const u8) error{ OutOfMemory, InvalidDataType }!usize {
        try store.has_availability();

        const exists = store.map.getPtr(key_data);
        if (exists) |value| {
            // ensure key stores a list
            if (!value.inner.is_list()) {
                return error.InvalidDataType;
            }

            const val = try store.values.reserve();
            try val.write(element);

            var item = try store.list_items.create();
            item.string = .{ .data = val };

            switch (direction) {
                .left => value.inner.list.linked.prepend(&item.node),
                .right => value.inner.list.linked.append(&item.node),
            }

            value.inner.list.len += 1;
            return value.inner.list.len;
        }

        // list does not exist
        // create a new item and append it to a new list
        const key = try store.keys.reserve();
        const val = try store.values.reserve();

        try key.write(key_data);
        try val.write(element);

        var item = try store.list_items.create();
        item.string = .{ .data = val };

        var list: List = .{ .linked = .{}, .len = 1 };
        list.linked.append(&item.node);

        store.map.putAssumeCapacity(key.slice(), .{ .key = key, .inner = .{ .list = list } });
        return list.len;
    }

    // Caller owns allocated memory upon a successful return. If an error occurs during processing,
    // this function will deinit the allocation, using the given allocator.
    pub fn range(store: *Store, alloc: std.mem.Allocator, key: []const u8, start: isize, stop: isize) error{ OutOfMemory, InvalidDataType }!AllocatedList {
        var items = AllocatedList.init();
        errdefer items.deinit(alloc);

        const value = store.get(key);
        if (value == null) {
            return items;
        }

        const inner = value.?.inner;
        if (!inner.is_list()) {
            return error.InvalidDataType;
        }

        const list = inner.list;

        const i_start: usize = start: {
            if (start >= list.len) {
                return items;
            }

            if (start < 0) {
                if (@abs(start) >= list.len) {
                    // trying to subtract past the length of the list
                    // force to 0
                    break :start 0;
                }

                break :start (list.len - @abs(start));
            }

            break :start @as(usize, @abs(start));
        };

        const i_stop: usize = stop: {
            // if stop is >= length of array, don't index past the array length
            if (stop >= list.len) {
                break :stop list.len;
            }

            if (stop < 0) {
                if (@abs(stop) >= list.len) {
                    // trying to subtract past the length of the list
                    // force to 0
                    break :stop 0;
                }

                break :stop (list.len - @abs(stop));
            }

            break :stop @as(usize, @abs(stop));
        };

        var current = list.linked.first;
        var i: usize = 0;
        while (current) |node| {
            if (i_start <= i and i <= i_stop) {
                const item: *ListItem = @fieldParentPtr("node", node);
                const copied = try alloc.dupe(u8, item.string.data.slice());

                try items.list.append(alloc, copied);
            }

            current = node.next;
            i += 1;
        }

        return items;
    }

    pub fn pop(store: *Store, alloc: std.mem.Allocator, key: []const u8, count: usize) error{ OutOfMemory, InvalidDataType }!AllocatedList {
        var items = AllocatedList.init();
        errdefer items.deinit(alloc);

        const exists = store.map.getPtr(key);
        if (exists) |value| {
            if (!value.inner.is_list()) {
                return error.InvalidDataType;
            }

            for (0..count) |_| {
                if (value.inner.list.linked.popFirst()) |node| {
                    const item: *ListItem = @fieldParentPtr("node", node);

                    // copy value and append to return list
                    const copied = try alloc.dupe(u8, item.string.data.slice());
                    try items.list.append(alloc, copied);

                    // deallocate item out from stored list
                    store.values.release(@constCast(item.string.data));
                    store.list_items.destroy(item);

                    value.inner.list.len -= 1;
                }
            }
        }

        return items;
    }

    pub fn remove(store: *Store, key: []const u8) bool {
        const kv = store.map.fetchRemove(key) orelse return false;
        switch (kv.value.inner) {
            .string => |string| {
                store.values.release(@constCast(string.data));
            },
            .list => |list| {
                var node = list.linked.first;
                while (node) |n| {
                    const item: *ListItem = @fieldParentPtr("node", n);
                    node = n.next;

                    store.values.release(@constCast(item.string.data));
                    store.list_items.destroy(item);
                }
            },
        }

        store.keys.release(@constCast(kv.value.key));
        return true;
    }
};

test "basic set and get" {
    const alloc = std.testing.allocator;

    var store = try Store.init(alloc, 1);
    defer store.deinit(alloc);

    try store.set("zig", "test", .{});
    const value = store.get("zig").?.inner.string.data.slice();

    try std.testing.expectEqualSlices(u8, "test", value);
    try std.testing.expect(store.remove("zig"));
}

test "push, 1 element" {
    const alloc = std.testing.allocator;

    var store = try Store.init(alloc, 2);
    defer store.deinit(alloc);

    try std.testing.expectEqual(1, try store.push(.right, "new", "test"));
    try std.testing.expectEqual(1, try store.push(.right, "just", "testing"));
}

test "push, multiple elements" {
    const alloc = std.testing.allocator;

    var store = try Store.init(alloc, 10);
    defer store.deinit(alloc);

    try std.testing.expectEqual(1, try store.push(.right, "just", "testing"));

    try std.testing.expectEqual(1, try store.push(.right, "list", "a"));
    try std.testing.expectEqual(2, try store.push(.right, "list", "b"));
    try std.testing.expectEqual(3, try store.push(.right, "list", "c"));
    try std.testing.expectEqual(4, try store.push(.right, "list", "d"));
    try std.testing.expectEqual(5, try store.push(.right, "list", "e"));
    try std.testing.expectEqual(6, try store.push(.left, "list", "f"));
    try std.testing.expectEqual(7, try store.push(.left, "list", "g"));
    try std.testing.expectEqual(8, try store.push(.left, "list", "h"));
    try std.testing.expectEqual(9, try store.push(.left, "list", "i"));
}

test "pop" {
    const alloc = std.testing.allocator;

    var store = try Store.init(alloc, 10);
    defer store.deinit(alloc);

    _ = try store.push(.right, "list", "a");
    _ = try store.push(.right, "list", "b");
    _ = try store.push(.right, "list", "c");

    var items = try store.pop(alloc, "list", 1);
    defer items.deinit(alloc);

    try std.testing.expectEqual(1, items.list.items.len);

    const value = store.get("list").?;
    try std.testing.expectEqual(2, value.inner.list.len);

    const first: *ListItem = @fieldParentPtr("node", value.inner.list.linked.first.?);
    try std.testing.expectEqualSlices(u8, "b", first.string.data.slice());

    const second: *ListItem = @fieldParentPtr("node", value.inner.list.linked.first.?.next.?);
    try std.testing.expectEqualSlices(u8, "c", second.string.data.slice());
}
