const std = @import("std");
const log10 = std.math.log10;

pub const Config = struct {
    /// Maximum number of concurrent connections.
    connections_max: u32,

    /// Key count is the number of possible keys we can store.
    /// Internally, the store will allocate (key_count * list_length_max) number
    /// of values, such that each key could support the maximum number of list elements.
    key_count: u32,

    /// The maximum allowable key size in bytes.
    key_size_max: u32,
    /// The maximum allowable value size in bytes.
    val_size_max: u32,

    /// The maximum allowable length for a list as a value.
    list_length_max: u32,

    pub fn debug(config: Config) void {
        const fmt =
            \\config
            \\  connections_max = {d}
            \\  key_count       = {d}
            \\  key_size_max    = {d}
            \\  val_size_max    = {d}
            \\  list_length_max = {d}
            \\
        ;

        std.debug.print(fmt, .{
            config.connections_max,
            config.key_count,
            config.key_size_max,
            config.val_size_max,
            config.list_length_max,
        });
    }
};

/// Allocation is a calculated set of values (in bytes), based on the given configuration.
/// This informs static allocation requested at initialization.
pub const Allocation = struct {
    connection_recv_size: u64,
    connection_send_size: usize,
    working_buffer_size: usize,

    pub fn from(config: Config) Allocation {
        const K = config.key_size_max;
        const V = config.val_size_max;
        const L = config.list_length_max;

        // connection recv size
        // Must support the largest possible RESP-formatted command.
        // Currently, this is a RPUSH/LPUSH with a key and maximum number of list elements.
        //
        // NOTE: The log_10() + 1 calculation is used to determine
        // the number of digits required to represent that number.
        const connection_recv_size: u64 =
            ("*".len + log10(L) + 1 + "\r\n".len) +
            ("$".len + "5".len + "\r\n".len + "RPUSH".len + "\r\n".len) +
            ("$".len + log10(K) + 1 + "\r\n".len + K + "\r\n".len) +
            (L * ("$".len + log10(V) + 1 + "\r\n".len + V + "\r\n".len));

        // connection send size
        // Must support the largest possible RESP-formatted response.
        // i.e. The largest possible list we can return.
        const connection_send_size: u64 =
            ("*".len + log10(L) + 1 + "\r\n".len) +
            (L * ("$".len + log10(V) + 1 + "\r\n".len + V + "\r\n".len));

        // working buffer size
        // The amount of space needed for parsing and executing commands.
        //
        // Parsing commands requires allocating a list of []const u8 slices
        // capable of holding the largest possible command.
        //
        // Executing (some) commands requires allocating a list of duplicated values
        // from the store. We have to duplicate values before writing to the send
        // buffer in the event there is a delay in sending. The duplicated list
        // is an ArrayList([]const u8) pointing to the actual duplicated values.
        const working_buffer_size: u64 =
            ((1 + 1 + L) * @sizeOf([]const u8)) + // ArrayList([]const u8) of largest possible command
            (L * @sizeOf([]const u8)) + (L * V); // ArrayList([]const u8) pointing to duplicated values

        return .{
            .connection_recv_size = connection_recv_size,
            .connection_send_size = connection_send_size,
            .working_buffer_size = working_buffer_size,
        };
    }

    pub fn debug(alloc: Allocation) void {
        const fmt =
            \\allocation
            \\  connection_recv_size = {d}
            \\  connection_send_size = {d}
            \\  working_buffer_size  = {d}
            \\
        ;

        std.debug.print(fmt, .{
            alloc.connection_recv_size,
            alloc.connection_send_size,
            alloc.working_buffer_size,
        });
    }
};
