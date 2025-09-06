const std = @import("std");
const testing = std.testing;

const command = @import("command.zig");
const store = @import("store.zig");
const Store = store.Store;
const SystemTimer = store.SystemTimer;
const Timer = store.Timer;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        _ = gpa.deinit();
    }

    const alloc = gpa.allocator();

    const address = try std.net.Address.resolveIp("127.0.0.1", 6379);
    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    var system = SystemTimer{};
    var timer = Timer{ .system = &system };

    var kv = Store.init(alloc, &timer); // TODO: use a fixed size allocator, with config option for size
    defer kv.deinit();

    std.debug.print("ready!\n", .{});

    while (true) {
        const conn = try listener.accept();
        _ = try std.Thread.spawn(.{}, handle_connection, .{ conn, &kv });
    }
}

fn handle_connection(conn: std.net.Server.Connection, kv: *Store) !void {
    var buffer: [4096]u8 = undefined; // TODO: think about correct buffer size here...
    var fba = std.heap.FixedBufferAllocator.init(&buffer);

    const alloc = fba.allocator();
    errdefer fba.reset();

    while (true) {
        const request = try alloc.alloc(u8, 512);
        const n = try conn.stream.read(request);
        if (n == 0) {
            break; // closed by client
        }

        var cmd = try command.parse(alloc, request[0..n]);
        const resp = try cmd.do(alloc, kv);
        if (resp) |r| {
            try conn.stream.writeAll(r);
        }

        // reset buffer for next request
        fba.reset();
    }

    conn.stream.close();
}

test {
    // some magic to get all unit tests to execute when running: `zig build test`
    // https://ziggit.dev/t/getting-zig-build-test-to-find-all-the-tests-in-my-module/6276
    // TODO(nickmonad): learn more about the build system
    std.testing.refAllDecls(@This());
}
