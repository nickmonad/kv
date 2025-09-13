const std = @import("std");
const testing = std.testing;

const mem = std.mem;
const net = std.net;
const posix = std.posix;
const linux = std.os.linux;

const command = @import("command.zig");
const store = @import("store.zig");
const Store = store.Store;
const SystemTimer = store.SystemTimer;
const Timer = store.Timer;

const PORT = 6379;
const IO_URING_ENTIRES = 1024;

const RequestType = enum {
    accept,
    recv,
    send,
};

const Request = struct {
    requestType: RequestType,
    client: posix.fd_t,
    buffer: []u8,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const alloc = gpa.allocator();
    defer {
        _ = gpa.deinit();
    }

    // init
    const socket = try listen(PORT);
    const cqes = try alloc.alloc(linux.io_uring_cqe, 32);
    var ring = try linux.IoUring.init(IO_URING_ENTIRES, 0);

    // peer address
    // only valid after successful CQE for an accept call
    var peer_addr: net.Address = .{ .any = undefined };
    var peer_addr_size: u32 = @sizeOf(posix.sockaddr);

    // submit initial accept
    var request = try alloc.create(Request);
    request.requestType = .accept;

    _ = try ring.accept(@intFromPtr(request), socket, &peer_addr.any, &peer_addr_size, 0);
    std.debug.assert(try ring.submit() > 0);

    std.debug.print("starting!\n", .{});
    while (true) {
        const num_cqe = try ring.copy_cqes(cqes, 1);
        std.debug.assert(num_cqe > 0);

        for (cqes[0..num_cqe]) |cqe| {
            std.debug.assert(cqe.user_data != 0);

            const req = @as(*Request, @ptrFromInt(cqe.user_data));
            switch (req.requestType) {
                .accept => {
                    std.debug.print("event: accept\n", .{});
                    // queue another accept request
                    var accept = try alloc.create(Request);
                    accept.requestType = .accept;

                    // queue recv request for connection
                    const client = cqe.res;

                    var recv = try alloc.create(Request);
                    recv.requestType = .recv;
                    recv.buffer = try alloc.alloc(u8, 1024);
                    recv.client = client;

                    _ = try ring.accept(@intFromPtr(accept), socket, &peer_addr.any, &peer_addr_size, 0);
                    _ = try ring.recv(@intFromPtr(recv), recv.client, .{ .buffer = recv.buffer }, 0);

                    std.debug.assert(try ring.submit() > 0);
                },
                .recv => {
                    std.debug.print("event: recv\n", .{});

                    std.debug.assert(cqe.res >= 0);
                    const len: usize = @intCast(cqe.res);

                    std.debug.print("{s}", .{req.buffer[0..len]});

                    var send = try alloc.create(Request);
                    send.requestType = .send;
                    send.buffer = req.buffer;
                    send.client = req.client;

                    _ = try ring.send(@intFromPtr(send), send.client, send.buffer[0..len], 0);

                    std.debug.assert(try ring.submit() > 0);
                },
                .send => {
                    // finally free buffer
                    alloc.free(req.buffer);
                    posix.close(req.client); // TODO: should prob be an SQE to the ring?
                },
            }

            alloc.destroy(req);
        }
    }

    // const address = try std.net.Address.resolveIp("127.0.0.1", 6379);
    // var listener = try address.listen(.{
    //     .reuse_address = true,
    // });
    // defer listener.deinit();

    // var system = SystemTimer{};
    // var timer = Timer{ .system = &system };

    // var kv = Store.init(alloc, &timer); // TODO: use a fixed size allocator, with config option for size
    // defer kv.deinit();

    // std.debug.print("ready!\n", .{});

    // while (true) {
    //     const conn = try listener.accept();
    //     _ = try std.Thread.spawn(.{}, handle_connection, .{ conn, &kv });
    // }
}

fn listen(port: u16) !posix.socket_t {
    const sockfd = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    errdefer posix.close(sockfd);

    try posix.setsockopt(sockfd, posix.SOL.SOCKET, posix.SO.REUSEPORT, &mem.toBytes(@as(c_int, 1)));

    const addr = try net.Address.parseIp4("0.0.0.0", port);

    try posix.bind(sockfd, &addr.any, @sizeOf(posix.sockaddr.in));
    try posix.listen(sockfd, std.math.maxInt(u31));

    return sockfd;
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
