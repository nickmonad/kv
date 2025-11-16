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

const buffer_pool = @import("buffer_pool.zig");
const Buffer = buffer_pool.Buffer;
const BufferPool = buffer_pool.BufferPool;

const command = @import("command.zig");
const store = @import("store.zig");
const Store = store.Store;

const PORT = 6379;
const IO_URING_ENTIRES = 1024;

const Connection = struct {
    client: posix.socket_t = undefined,

    recv_buffer: *Buffer,
    send_buffer: *Buffer,

    completion: Completion = undefined,

    fn valid(c: Connection) bool {
        return c.recv_buffer.reserved and c.send_buffer.reserved;
    }
};

const Completion = struct {
    result: i32 = undefined,
    operation: Operation,

    fn prepare(completion: *Completion, sqe: *io_uring_sqe) void {
        switch (completion.operation) {
            .accept => |*accept| {
                sqe.prep_accept(accept.socket, &accept.address, &accept.address_len, 0);
            },
            .recv => |recv| {
                sqe.prep_recv(recv.socket, recv.buffer, 0);
            },
            .send => |send| {
                sqe.prep_send(send.socket, send.buffer[0..send.length], 0);
            },
            .close => |close| {
                sqe.prep_close(close.socket);
            },
        }

        sqe.user_data = @intFromPtr(completion);
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
    close: struct {
        socket: posix.socket_t,
    },
};

const ConnectionPool = struct {
    const Pool = std.heap.MemoryPoolExtra(Connection, .{ .growable = false });

    buffers: BufferPool,
    connections: Pool,

    fn init(gpa: std.mem.Allocator, max_connections: u32) !ConnectionPool {
        const pool = try Pool.initPreheated(gpa, max_connections);
        const buffers = try BufferPool.init(gpa, max_connections * 2, 4096);

        return .{
            .buffers = buffers,
            .connections = pool,
        };
    }

    fn deinit(pool: *ConnectionPool) void {
        pool.buffers.deinit();
        pool.connections.deinit();
    }

    fn create(pool: *ConnectionPool) !*Connection {
        const connection = try pool.connections.create();
        const recv = try pool.buffers.reserve();
        const send = try pool.buffers.reserve();

        connection.recv_buffer = recv;
        connection.send_buffer = send;

        return connection;
    }

    fn destroy(pool: *ConnectionPool, connection: *Connection) void {
        pool.buffers.release(connection.recv_buffer);
        pool.buffers.release(connection.send_buffer);

        pool.connections.destroy(connection);
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

    fn deinit(server: *Server) void {
        server.ring.deinit();
    }

    fn run(server: *Server) !void {
        assert(server.completion.operation == .accept);

        // start server with initial accept
        const accept = try server.ring.get_sqe();
        server.completion.prepare(accept);

        // main loop!
        // submit any outstanding SQEs,
        // wait for up to some batched number of CQEs,
        // process them,
        // rinse and repeat
        while (true) {
            _ = try server.ring.submit_and_wait(1);

            while (server.ring.cq_ready() > 0) {
                const completed = try server.ring.copy_cqes(&server.cqes, 1);

                for (server.cqes[0..completed]) |cqe| {
                    assert(cqe.user_data != 0);

                    const completion: *Completion = @ptrFromInt(cqe.user_data);

                    // TODO(nickmonad) handle errors here, as tigerbeetle does
                    // on_* functions should take ErrorType!usize or whatever they need from cqe.res
                    switch (completion.operation) {
                        .accept => {
                            const client: posix.fd_t = @intCast(cqe.res);
                            server.on_accept(client);
                        },
                        .recv => {
                            const n: usize = @intCast(cqe.res);
                            const connection: *Connection = @fieldParentPtr("completion", completion);
                            server.on_recv(connection, n);
                        },
                        .send => {
                            const connection: *Connection = @fieldParentPtr("completion", completion);
                            server.on_send(connection);
                        },
                        .close => {
                            const connection: *Connection = @fieldParentPtr("completion", completion);
                            server.on_close(connection);
                        },
                    }
                }
            }
        }
    }

    fn on_accept(server: *Server, client: posix.socket_t) void {
        assert(server.completion.operation == .accept);

        var connection = server.connections.create() catch {
            // TODO(nickmonad)
            // use a statically allocated buffer for error response to the client here
            // also be sure to cleanup the socket resource
            std.debug.panic("OOM for new connection! need to kick error back to client", .{});
        };

        assert(connection.valid());

        connection.client = client;
        connection.completion = .{
            .operation = .{
                .recv = .{
                    .socket = client,
                    .buffer = connection.recv_buffer.buf,
                },
            },
        };

        // TODO(nickmonad) enqueue these if no more SQEs are available
        const recv = server.ring.get_sqe() catch unreachable;
        connection.completion.prepare(recv);

        const accept = server.ring.get_sqe() catch unreachable;
        server.completion.prepare(accept);
    }

    fn on_recv(server: *Server, connection: *Connection, read: usize) void {
        assert(connection.completion.operation == .recv);
        assert(connection.valid());

        if (read == 0) {
            // connection closed by client, cleanup
            connection.completion = .{
                .operation = .{
                    .close = .{
                        .socket = connection.client,
                    },
                },
            };

            // TODO enqueue these if no more SQEs are available
            const close = server.ring.get_sqe() catch unreachable;
            connection.completion.prepare(close);

            return;
        }

        // wrap send_buffer in allocator
        // command will "write" to this allocator
        // TODO(nickmonad) handle command failure (OOM or otherwise?)
        var output: Writer = .fixed(connection.send_buffer.buf);

        const alloc = server.fba.allocator();
        defer server.fba.reset();

        // TODO(nickmonad) handle parsing error
        var cmd = command.parse(alloc, connection.recv_buffer.buf[0..read]) catch unreachable;
        cmd.do(alloc, server.kv, &output) catch unreachable;

        connection.completion = .{
            .operation = .{
                .send = .{
                    .socket = connection.client,
                    .buffer = connection.send_buffer.buf,
                    .length = output.buffered().len,
                },
            },
        };

        // TODO(nickmonad)
        // same situation here as in on_recv, need to queue these up somehow
        const send = server.ring.get_sqe() catch unreachable;
        connection.completion.prepare(send);
    }

    fn on_send(server: *Server, connection: *Connection) void {
        assert(connection.completion.operation == .send);
        assert(connection.valid());

        // send complete!
        // keep connection alive, check for recv from client
        connection.completion.operation = .{
            .recv = .{
                .socket = connection.client,
                .buffer = connection.recv_buffer.buf,
            },
        };

        // TODO(nickmond)
        // again, queue these up if the submissions are full
        const recv = server.ring.get_sqe() catch unreachable;
        connection.completion.prepare(recv);
    }

    fn on_close(server: *Server, connection: *Connection) void {
        assert(connection.valid());
        server.connections.destroy(connection);
    }
};

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{ .enable_memory_limit = true }) = .init;
    defer assert(gpa.deinit() == .ok);

    // TODO(nickmonad)
    // buffer size configuration
    // max connection configuration
    const buffer = try gpa.allocator().alloc(u8, 1024 * 1024);
    defer gpa.allocator().free(buffer);
    const fba = std.heap.FixedBufferAllocator.init(buffer);

    var pool = try ConnectionPool.init(gpa.allocator(), 10);
    defer pool.deinit();

    // TODO(nickmonad) config store size
    var kv = try Store.init(gpa.allocator(), 1024);
    defer kv.deinit(gpa.allocator());

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

test "MemoryPoolExtra growable = false" {
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

test "ConnectionPool" {
    const alloc = std.testing.allocator;

    var pool = try ConnectionPool.init(alloc, 2);
    defer pool.deinit();

    // create (1) and (2)
    const c = try pool.create();
    _ = try pool.create();
    // fail on (3)
    try std.testing.expectError(error.OutOfMemory, pool.create());

    // destroy (1)
    pool.destroy(c);
    // create another
    _ = try pool.create();
    // maxed out, fail again
    try std.testing.expectError(error.OutOfMemory, pool.create());
}
