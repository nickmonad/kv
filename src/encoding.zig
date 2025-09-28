const std = @import("std");
const Writer = std.io.Writer;

pub const BulkString = struct {
    pub fn encode(w: *Writer, str: []const u8) !void {
        return w.print("${d}\r\n{s}\r\n", .{ str.len, str });
    }
};

pub const BulkArray = struct {
    pub fn encode(w: *Writer, elements: []const []const u8) !void {
        _ = try w.print("*{d}\r\n", .{elements.len});

        for (elements) |element| {
            _ = try w.print("${d}\r\n{s}\r\n", .{ element.len, element });
        }
    }
};

test "BulkString encode" {
    var buf: [32]u8 = undefined;
    var writer: Writer = .fixed(&buf);

    try BulkString.encode(&writer, "");
    try std.testing.expectEqualSlices(u8, "$0\r\n\r\n", writer.buffered());
    writer.undo(writer.buffered().len);

    try BulkString.encode(&writer, "example");
    try std.testing.expectEqualSlices(u8, "$7\r\nexample\r\n", writer.buffered());
}

test "BulkArray encode" {
    var buf: [100]u8 = undefined;
    var writer: Writer = .fixed(&buf);

    try BulkArray.encode(&writer, &.{});
    try std.testing.expectEqualSlices(u8, "*0\r\n", writer.buffered());
    writer.undo(writer.buffered().len);

    try BulkArray.encode(&writer, &.{ "test", "array" });
    try std.testing.expectEqualSlices(u8, "*2\r\n$4\r\ntest\r\n$5\r\narray\r\n", writer.buffered());
    writer.undo(writer.buffered().len);

    try BulkArray.encode(&writer, &.{ "test", "array", "three" });
    try std.testing.expectEqualSlices(u8, "*3\r\n$4\r\ntest\r\n$5\r\narray\r\n$5\r\nthree\r\n", writer.buffered());
}
