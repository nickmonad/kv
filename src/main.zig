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
const Writer = std.io.Writer;

const command = @import("command.zig");
const store = @import("store.zig");
const Store = store.Store;
const SystemTimer = store.SystemTimer;
const Timer = store.Timer;

const PORT = 6379;
const IO_URING_ENTIRES = 1024;

const Connection = struct {
    client: posix.socket_t = undefined,

    recv_buffer: []u8 = undefined,
    send_buffer: []u8 = undefined,

    completion: Completion = undefined,
};

const Completion = struct {
    result: i32 = undefined,
    operation: Operation,

    fn prepare(self: *Completion, sqe: *io_uring_sqe) void {
        switch (self.operation) {
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

const Operation = union(enum) {
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
};

// TODO(nickmonad)
// unit tests for this guy
const ConnectionPool = struct {
    connections: std.heap.MemoryPoolExtra(Connection, .{ .growable = false }),

    recv_alloc: std.heap.FixedBufferAllocator,
    send_alloc: std.heap.FixedBufferAllocator,

    // used during deinit()
    gpa: std.mem.Allocator,
    recv_buffers: []u8,
    send_buffers: []u8,

    fn init(gpa: std.mem.Allocator, max: usize) !ConnectionPool {
        // each connection has one recv and send buffer (each 4 KB)
        // TODO(nickmonad): make per buffer size configurable
        // NOTE(nickmonad): these may not necessarily need to be separate allocation "spaces", we could
        // just have one massive buffer space (max * 2 * 4 KB) (maybe?)
        const recv_buffers = try gpa.alloc(u8, max * 4096);
        const send_buffers = try gpa.alloc(u8, max * 4096);

        return .{
            .connections = try std.heap.MemoryPoolExtra(Connection, .{ .growable = false }).initPreheated(gpa, max),
            .recv_alloc = std.heap.FixedBufferAllocator.init(recv_buffers),
            .send_alloc = std.heap.FixedBufferAllocator.init(send_buffers),
            .gpa = gpa,
            .recv_buffers = recv_buffers,
            .send_buffers = send_buffers,
        };
    }

    fn deinit(self: *ConnectionPool) void {
        self.gpa.free(self.recv_buffers);
        self.gpa.free(self.send_buffers);

        self.connections.deinit();
    }

    fn create(self: *ConnectionPool) !*Connection {
        const new: *Connection = try self.connections.create();

        // Invariant!
        // Buffer space is determined by number of connections since they are always allocated together here.
        // If we can get a connection, we can get recv and send buffers.
        // TODO(nickmonad): store buffer size on struct
        const recv_buffer = self.recv_alloc.allocator().alloc(u8, 4096) catch unreachable;
        const send_buffer = self.send_alloc.allocator().alloc(u8, 4096) catch unreachable;

        new.recv_buffer = recv_buffer;
        new.send_buffer = send_buffer;

        return new;
    }

    fn destroy(self: *ConnectionPool, connection: *Connection) void {
        // TODO(nickmonad) this should be a SQE to prevent blocking thread on syscall
        _ = linux.close(connection.client);

        // ensure buffer space is returned
        self.recv_alloc.allocator().free(connection.recv_buffer);
        self.send_alloc.allocator().free(connection.send_buffer);

        self.connections.destroy(connection);
    }
};

/// Single-threaded server, with async I/O backed by io_uring.
/// Inspired partially by TigerBeetle, with some simplification.
/// Server operation is tied pretty closely with the "event loop". While there are certainly
/// more expressive ways to handle different "kinds" of I/O operations, we're focused on simplicity for now.
const Server = struct {
    fba: std.heap.FixedBufferAllocator,
    connections: *ConnectionPool,

    completion: Completion,

    ring: IO_Uring,
    cqes: [32]linux.io_uring_cqe = undefined,

    kv: *Store,

    fn init(fba: std.heap.FixedBufferAllocator, pool: *ConnectionPool, kv: *Store) !Server {
        const socket = try listen(PORT);

        return Server{
            .fba = fba,
            .connections = pool,
            .completion = .{ .operation = .{ .accept = .{
                .socket = socket,
                .address = undefined,
                .address_len = @sizeOf(posix.sockaddr),
            } } },
            .ring = try IO_Uring.init(IO_URING_ENTIRES, 0),
            .kv = kv,
        };
    }

    fn deinit(self: *Server) void {
        self.ring.deinit();
    }

    fn run(self: *Server) !void {
        assert(self.completion.operation == .accept);

        // start server with initial accept
        const accept = try self.ring.get_sqe();
        self.completion.prepare(accept);

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

                    const completion: *Completion = @ptrFromInt(cqe.user_data);

                    // TODO(nickmonad) handle errors here, as tigerbeetle does
                    // on_* functions should take ErrorType!usize or whatever they need from cqe.res
                    switch (completion.operation) {
                        .accept => {
                            const client: posix.fd_t = @intCast(cqe.res);
                            self.on_accept(client);
                        },
                        .recv => {
                            const n: usize = @intCast(cqe.res);
                            const connection: *Connection = @fieldParentPtr("completion", completion);
                            self.on_recv(connection, n);
                        },
                        .send => {
                            const connection: *Connection = @fieldParentPtr("completion", completion);
                            self.on_send(connection);
                        },
                    }
                }
            }
        }
    }

    fn on_accept(self: *Server, client: posix.socket_t) void {
        assert(self.completion.operation == .accept);
        const connection = self.connections.create() catch {
            // TODO(nickmonad)
            // use a statically allocated buffer for error response to the client here
            // also be sure to cleanup the socket resource
            std.debug.panic("OOM for new connection! need to kick error back to client", .{});
        };

        connection.client = client;
        connection.completion = .{
            .operation = .{
                .recv = .{
                    .socket = client,
                    .buffer = connection.recv_buffer,
                },
            },
        };

        // TODO(nickmonad) enqueue these if no more SQEs are available
        const recv = self.ring.get_sqe() catch unreachable;
        connection.completion.prepare(recv);

        const accept = self.ring.get_sqe() catch unreachable;
        self.completion.prepare(accept);
    }

    fn on_recv(self: *Server, connection: *Connection, read: usize) void {
        assert(connection.completion.operation == .recv);
        if (read == 0) {
            // connection closed by client, cleanup
            self.close(connection);
            return;
        }

        // wrap send_buffer in allocator
        // command will "write" to this allocator
        // TODO(nickmonad) handle command failure (OOM or otherwise?)
        var output: Writer = .fixed(connection.send_buffer);

        const alloc = self.fba.allocator();
        defer self.fba.reset();

        // TODO(nickmonad) handle parsing error
        var cmd = command.parse(alloc, connection.recv_buffer[0..read]) catch unreachable;
        cmd.do(alloc, self.kv, &output) catch unreachable;

        connection.completion.operation = .{
            .send = .{
                .socket = connection.client,
                .buffer = connection.send_buffer,
                .length = output.buffered().len,
            },
        };

        // TODO(nickmonad)
        // same situation here as in on_recv, need to queue these up somehow
        const send = self.ring.get_sqe() catch unreachable;
        connection.completion.prepare(send);
    }

    fn on_send(self: *Server, connection: *Connection) void {
        assert(connection.completion.operation == .send);

        // send complete!
        // keep connection alive, check for recv from client
        connection.completion.operation = .{
            .recv = .{
                .socket = connection.client,
                .buffer = connection.recv_buffer,
            },
        };

        // TODO(nickmond)
        // again, queue these up if the submissions are full
        const recv = self.ring.get_sqe() catch unreachable;
        connection.completion.prepare(recv);
    }

    fn close(self: *Server, connection: *Connection) void {
        self.connections.destroy(connection);
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer assert(gpa.deinit() == .ok);

    // TODO(nickmonad)
    // buffer size configuration
    // max connection configuration
    const buffer = try gpa.allocator().alloc(u8, 1024 * 1024);
    defer gpa.allocator().free(buffer);
    const fba = std.heap.FixedBufferAllocator.init(buffer);

    var pool = try ConnectionPool.init(gpa.allocator(), 10);
    defer pool.deinit();

    var system = SystemTimer{};
    var timer = Timer{ .system = &system };

    // TODO(nickmonad)
    // use an allocator with upper bound for store, make configurable
    var kv = Store.init(gpa.allocator(), &timer);
    defer kv.deinit();

    // TODO(nickmonad)
    // handle graceful shutdown from SIGTERM
    var server = try Server.init(fba, &pool, &kv);
    std.debug.print("ready!\n", .{});
    try server.run();
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

test {
    // some magic to get all unit tests to execute when running: `zig build test`
    // https://ziggit.dev/t/getting-zig-build-test-to-find-all-the-tests-in-my-module/6276
    // TODO(nickmonad): learn more about the build system
    std.testing.refAllDecls(@This());
}

test "memory pool growable = false" {
    const alloc = std.testing.allocator;

    var pool = try std.heap.MemoryPoolExtra(Connection, .{ .growable = false }).initPreheated(alloc, 5);
    defer pool.deinit();

    const c = try pool.create();
    _ = try pool.create();
    _ = try pool.create();
    _ = try pool.create();
    _ = try pool.create();

    try std.testing.expectError(error.OutOfMemory, pool.create());
    try std.testing.expectError(error.OutOfMemory, pool.create());

    pool.destroy(c);

    _ = try pool.create();

    try std.testing.expectError(error.OutOfMemory, pool.create());
}
