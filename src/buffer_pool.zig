const std = @import("std");
const assert = std.debug.assert;

pub const Buffer = struct {
    // internal
    // use write(), do not set directly!
    buf: []u8 = undefined,

    // length of the written content
    len: usize = 0,

    // internal
    next: ?*Buffer = null,
    reserved: bool = false,
    owner: ?*BufferPool = null,

    pub fn write(buffer: *Buffer, data: []const u8) error{OutOfMemory}!void {
        assert(buffer.reserved);

        if (data.len > buffer.buf.len) {
            // TODO(nickmonad) this should probably be a different error
            // something like "WillOverflow". I think the OOM should likely be
            // reserved for the pool itself, when it has no more buffers to give.
            return error.OutOfMemory;
        }

        @memcpy(buffer.buf[0..data.len], data);
        buffer.len = data.len;
    }

    pub fn slice(buffer: *const Buffer) []const u8 {
        return buffer.buf[0..buffer.len];
    }
};

pub const BufferPool = struct {
    arena: std.heap.ArenaAllocator,
    free: ?*Buffer = null,

    pub fn init(gpa: std.mem.Allocator, num: u32, buffer_size: usize) !BufferPool {
        var arena = std.heap.ArenaAllocator.init(gpa);
        var free: ?*Buffer = null;

        const alloc = arena.allocator();
        for (0..num) |_| {
            const buffer = try alloc.create(Buffer);
            const buf = try alloc.alloc(u8, buffer_size);

            buffer.* = .{
                .buf = buf,
                .next = free,
            };

            free = buffer;
        }

        return .{
            .arena = arena,
            .free = free,
        };
    }

    pub fn deinit(pool: *BufferPool) void {
        pool.arena.deinit();
    }

    pub fn reserve(pool: *BufferPool) error{OutOfMemory}!*Buffer {
        if (pool.free) |buffer| {
            pool.free = buffer.next;

            assert(buffer.len == 0);
            assert(!buffer.reserved);

            buffer.owner = pool;
            buffer.next = null;
            buffer.reserved = true;

            return buffer;
        }

        return error.OutOfMemory;
    }

    pub fn release(pool: *BufferPool, buffer: *Buffer) void {
        assert(buffer.owner == pool);

        buffer.len = 0;
        buffer.reserved = false;
        buffer.next = pool.free;

        pool.free = buffer;
    }
};

test "basic usage" {
    const alloc = std.testing.allocator;
    const buffer_size = 1024;

    var pool = try BufferPool.init(alloc, 5, buffer_size);
    defer pool.deinit();

    // reserve buffer (1) and check properties
    const buffer = try pool.reserve();

    try std.testing.expectEqual(buffer_size, buffer.buf.len);

    // release buffer (1)
    pool.release(buffer);

    // reserve all buffers, ensure OOM when at max
    _ = try pool.reserve();
    _ = try pool.reserve();
    _ = try pool.reserve();
    _ = try pool.reserve();
    _ = try pool.reserve();

    try std.testing.expectError(error.OutOfMemory, pool.reserve());
}

test "sanity check" {
    const alloc = std.testing.allocator;
    const buffer_size = 1024;

    var pool = try BufferPool.init(alloc, 5, buffer_size);
    defer pool.deinit();

    // reserve buffer (1) and set data
    const buffer = try pool.reserve();
    buffer.buf[0] = 'z';

    // release buffer (1)
    pool.release(buffer);

    // reserve buffer again, should be same as (1)
    // ... not that we would ever depend on this!
    const again = try pool.reserve();
    try std.testing.expectEqual('z', again.buf[0]);
}

test "buffer write" {
    const alloc = std.testing.allocator;
    const buffer_size = 1024;

    var pool = try BufferPool.init(alloc, 1, buffer_size);
    defer pool.deinit();

    const buffer = try pool.reserve();
    defer pool.release(buffer);

    try buffer.write("test");
    try std.testing.expectEqual(4, buffer.len);
    try std.testing.expectEqualSlices(u8, "test", buffer.slice());

    try buffer.write("overwrite");
    try std.testing.expectEqual(9, buffer.len);
    try std.testing.expectEqualSlices(u8, "overwrite", buffer.slice());

    const max: [buffer_size]u8 = @splat('a');
    try buffer.write(&max);
    try std.testing.expectEqual(1024, buffer.len);

    const overflow: [buffer_size + 1]u8 = @splat('a');
    try std.testing.expectError(error.OutOfMemory, buffer.write(&overflow));
}
