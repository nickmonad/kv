const std = @import("std");
const assert = std.debug.assert;

pub const Error = error{
    DataOverflow,
    OutOfMemory,
};

pub const ByteArray = struct {
    // internal
    // use write(), do not set directly!
    data: []u8 = undefined,

    // used length of the written content
    // separate from the data.len, which is the total available space
    used: usize = 0,

    // internal
    next: ?*ByteArray = null,
    reserved: bool = false,
    owner: ?*ByteArrayPool = null,

    pub fn write(bytes: *ByteArray, data: []const u8) Error!void {
        assert(bytes.reserved);

        if (data.len > bytes.data.len) {
            return Error.DataOverflow;
        }

        @memcpy(bytes.data[0..data.len], data);
        bytes.used = data.len;
    }

    pub fn slice(bytes: *const ByteArray) []const u8 {
        return bytes.data[0..bytes.used];
    }
};

pub const ByteArrayPool = struct {
    arena: std.heap.ArenaAllocator,
    free: ?*ByteArray = null,

    pub fn init(gpa: std.mem.Allocator, num: u64, data_size: usize) Error!ByteArrayPool {
        var arena = std.heap.ArenaAllocator.init(gpa);
        var free: ?*ByteArray = null;

        const alloc = arena.allocator();
        for (0..num) |_| {
            const bytes = try alloc.create(ByteArray);
            const data = try alloc.alloc(u8, data_size);

            bytes.* = .{
                .data = data,
                .next = free,
            };

            free = bytes;
        }

        return .{
            .arena = arena,
            .free = free,
        };
    }

    pub fn deinit(pool: *ByteArrayPool) void {
        pool.arena.deinit();
    }

    pub fn reserve(pool: *ByteArrayPool) error{OutOfMemory}!*ByteArray {
        if (pool.free) |bytes| {
            pool.free = bytes.next;

            assert(bytes.used == 0);
            assert(!bytes.reserved);

            bytes.owner = pool;
            bytes.next = null;
            bytes.reserved = true;

            return bytes;
        }

        return error.OutOfMemory;
    }

    pub fn release(pool: *ByteArrayPool, bytes: *ByteArray) void {
        assert(bytes.owner == pool);

        bytes.used = 0;
        bytes.reserved = false;
        bytes.next = pool.free;

        pool.free = bytes;
    }
};

test "basic usage" {
    const alloc = std.testing.allocator;
    const bytes_size = 1024;

    var pool = try ByteArrayPool.init(alloc, 5, bytes_size);
    defer pool.deinit();

    // reserve bytes (1) and check properties
    const bytes = try pool.reserve();

    try std.testing.expectEqual(bytes_size, bytes.data.len);

    // release bytes (1)
    pool.release(bytes);

    // reserve all bytess, ensure OOM when at max
    _ = try pool.reserve();
    _ = try pool.reserve();
    _ = try pool.reserve();
    _ = try pool.reserve();
    _ = try pool.reserve();

    try std.testing.expectError(error.OutOfMemory, pool.reserve());
}

test "sanity check" {
    const alloc = std.testing.allocator;
    const bytes_size = 1024;

    var pool = try ByteArrayPool.init(alloc, 5, bytes_size);
    defer pool.deinit();

    // reserve bytes (1) and set data
    const bytes = try pool.reserve();
    bytes.data[0] = 'z';

    // release bytes (1)
    pool.release(bytes);

    // reserve bytes again, should be same as (1)
    // ... not that we would ever depend on this!
    const again = try pool.reserve();
    try std.testing.expectEqual('z', again.data[0]);
}

test "bytes write" {
    const alloc = std.testing.allocator;
    const bytes_size = 1024;

    var pool = try ByteArrayPool.init(alloc, 1, bytes_size);
    defer pool.deinit();

    const bytes = try pool.reserve();
    defer pool.release(bytes);

    try bytes.write("test");
    try std.testing.expectEqual(4, bytes.used);
    try std.testing.expectEqualSlices(u8, "test", bytes.slice());

    try bytes.write("overwrite");
    try std.testing.expectEqual(9, bytes.used);
    try std.testing.expectEqualSlices(u8, "overwrite", bytes.slice());

    const max: [bytes_size]u8 = @splat('a');
    try bytes.write(&max);
    try std.testing.expectEqual(1024, bytes.used);

    const overflow: [bytes_size + 1]u8 = @splat('a');
    try std.testing.expectError(Error.DataOverflow, bytes.write(&overflow));
}
