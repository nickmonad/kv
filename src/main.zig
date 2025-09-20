const std = @import("std");
const testing = std.testing;
const assert = std.debug.assert;

const mem = std.mem;
const net = std.net;
const posix = std.posix;
const linux = std.os.linux;
const IO_Uring = linux.IoUring;
const io_uring_sqe = linux.io_uring_sqe;
const io_uring_cqe = linux.io_uring_cqe;

const command = @import("command.zig");
const store = @import("store.zig");
const Store = store.Store;
const SystemTimer = store.SystemTimer;
const Timer = store.Timer;

const PORT = 6379;
const IO_URING_ENTIRES = 1024;

/// Single-threaded server, with async I/O backed by io_uring.
/// Inspired partially by TigerBeetle, with some simplification.
/// Server operation is tied pretty closely with the "event loop". While there are certainly
/// more expressive ways to handle different "kinds" of I/O operations, we're focused on simplicity for now.
const Server = struct {
    alloc: std.mem.Allocator,
    connection: Connection,

    ring: IO_Uring,
    cqes: [32]linux.io_uring_cqe = undefined,

    const Connection = union(enum) {
        accept: struct {
            socket: posix.socket_t,
            address: posix.sockaddr = undefined,
            address_len: posix.socklen_t = @sizeOf(posix.sockaddr),
        },
        recv: struct {
            socket: posix.socket_t,
            buffer: []u8,
        },
        send: struct {
            socket: posix.socket_t,
            buffer: []u8,
            length: usize,
        },

        fn prepare(self: *Connection, sqe: *io_uring_sqe) void {
            switch (self.*) {
                .accept => |*accept| {
                    sqe.prep_accept(accept.socket, &accept.address, &accept.address_len, 0);
                },
                .recv => |recv| {
                    sqe.prep_recv(recv.socket, recv.buffer, 0);
                },
                .send => |send| {
                    sqe.prep_send(send.socket, send.buffer[0..send.length], 0);
                },
            }

            sqe.user_data = @intFromPtr(self);
        }
    };

    fn init(gpa: std.mem.Allocator) !Server {
        const socket = try listen(PORT);

        return Server{
            .alloc = gpa,
            .connection = .{ .accept = .{
                .socket = socket,
                .address = undefined,
                .address_len = @sizeOf(posix.sockaddr),
            } },
            .ring = try IO_Uring.init(IO_URING_ENTIRES, 0),
        };
    }

    fn run(self: *Server) !void {
        assert(self.connection == .accept);

        // start server with initial accept
        const accept = try self.ring.get_sqe();
        self.connection.prepare(accept);

        // main loop!
        // submit any outstanding SQEs,
        // wait for up to some batched number of CQEs,
        // process them,
        // rinse and repeat
        while (true) {
            _ = try self.ring.submit_and_wait(1);

            while (self.ring.cq_ready() > 0) {
                const completed = try self.ring.copy_cqes(&self.cqes, 1);

                for (self.cqes[0..completed]) |cqe| {
                    assert(cqe.user_data != 0);

                    const connection: *Connection = @ptrFromInt(cqe.user_data);
                    // TODO(nickmonad) handle errors here, as tigerbeetle does
                    // on_* functions should take ErrorType!usize or whatever they need from cqe.res
                    switch (connection.*) {
                        .accept => {
                            const client: posix.fd_t = @intCast(cqe.res);
                            self.on_accept(client);
                        },
                        .recv => {
                            const n: usize = @intCast(cqe.res);
                            self.on_recv(connection, n);
                        },
                        .send => {
                            self.on_send(connection);
                        },
                    }
                }
            }
        }
    }

    fn on_accept(self: *Server, client: posix.socket_t) void {
        assert(self.connection == .accept);

        const connection = self.alloc.create(Connection) catch {
            // TODO(nickmonad)
            // use a statically allocated buffer for error response to client here
            // also be sure to cleanup the socket resource
            std.debug.panic("OOM for new connection! need to kick out error to client", .{});
        };

        // TODO(nickmonad)
        // we can maybe somehow tie the lifetime of these buffers to the connection?
        // if we can allocate a connection, we should also be able to allocate a buffer, and hence, this shouldn't fail
        const buffer = self.alloc.alloc(u8, 1024) catch unreachable;
        connection.* = .{
            .recv = .{
                .socket = client,
                .buffer = buffer,
            },
        };

        // TODO(nickmonad)
        // enqueue these somehow if none are available
        const recv = self.ring.get_sqe() catch unreachable;
        connection.prepare(recv);

        const accept = self.ring.get_sqe() catch unreachable;
        self.connection.prepare(accept);
    }

    fn on_recv(self: *Server, connection: *Connection, n: usize) void {
        assert(connection.* == .recv);

        if (n == 0) {
            // connection closed by client, cleanup
            self.close(connection);
            return;
        }

        const client = connection.recv.socket;
        const buffer = connection.recv.buffer;

        connection.* = .{
            .send = .{
                .socket = client,
                .buffer = buffer,
                .length = n,
            },
        };

        // TODO(nickmonad)
        // same situation here as in on_recv, need to queue these up somehow
        const send = self.ring.get_sqe() catch unreachable;
        connection.prepare(send);
    }

    fn on_send(self: *Server, connection: *Connection) void {
        assert(connection.* == .send);

        // send complete!
        // reuse connection, set to recv state
        const client = connection.send.socket;
        const buffer = connection.send.buffer;
        connection.* = .{
            .recv = .{
                .socket = client,
                .buffer = buffer,
            },
        };

        // TODO(nickmond)
        // again, queue these up if the submissions are full
        const recv = self.ring.get_sqe() catch unreachable;
        connection.prepare(recv);
    }

    fn close(self: *Server, connection: *Connection) void {
        var socket: posix.socket_t = undefined;
        var buffer: []u8 = undefined;

        switch (connection.*) {
            .recv => |recv| {
                socket = recv.socket;
                buffer = recv.buffer;
            },
            .send => |send| {
                socket = send.socket;
                buffer = send.buffer;
            },
            else => unreachable,
        }

        _ = linux.close(socket);
        self.alloc.free(buffer);
        self.alloc.destroy(connection);
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer assert(gpa.deinit() == .ok);

    const alloc = gpa.allocator();
    var server = try Server.init(alloc);

    std.debug.print("ready!\n", .{});
    try server.run();

    // // peer address
    // // only valid after successful CQE for an accept call
    // var peer_addr: net.Address = .{ .any = undefined };
    // var peer_addr_size: u32 = @sizeOf(posix.sockaddr);

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
    try posix.listen(sockfd, std.math.maxInt(u31)); // TODO: this should probably the same as our max client config

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
