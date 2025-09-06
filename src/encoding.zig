const std = @import("std");

pub const BulkString = struct {
    str: []const u8,

    const Self = @This();

    pub fn encode(alloc: std.mem.Allocator, str: []const u8) !Self {
        const s = try std.fmt.allocPrint(alloc, "${d}\r\n{s}\r\n", .{ str.len, str });
        return .{ .str = s };
    }
};

pub const BulkArray = struct {
    str: []const u8,

    const Self = @This();

    // encode
    // caller is responsible for freeing returned str
    pub fn encode(alloc: std.mem.Allocator, elements: []const []const u8) !Self {
        var encoded = try std.ArrayList([]const u8).initCapacity(alloc, elements.len + 1);
        defer encoded.deinit(alloc);

        const n = try std.fmt.allocPrint(alloc, "*{d}\r\n", .{elements.len});
        try encoded.append(alloc, n);

        for (elements) |element| {
            const e = try std.fmt.allocPrint(alloc, "${d}\r\n{s}\r\n", .{ element.len, element });
            try encoded.append(alloc, e);
        }

        const str = try std.mem.concat(alloc, u8, encoded.items);
        for (encoded.items) |item| {
            alloc.free(item);
        }

        return .{ .str = str };
    }
};

test "BulkString encode" {
    const alloc = std.testing.allocator;

    const empty = try BulkString.encode(alloc, "");
    defer alloc.free(empty.str);

    const example = try BulkString.encode(alloc, "example");
    defer alloc.free(example.str);

    try std.testing.expectEqualSlices(u8, "$0\r\n\r\n", empty.str);
    try std.testing.expectEqualSlices(u8, "$7\r\nexample\r\n", example.str);
}

test "BulkArray encode" {
    const alloc = std.testing.allocator;

    const empty = try BulkArray.encode(alloc, &[_][]const u8{});
    defer alloc.free(empty.str);

    const elements = try BulkArray.encode(alloc, &[_][]const u8{ "test", "array" });
    defer alloc.free(elements.str);

    const doesitwork = BulkArray.encode(alloc, &.{ "test", "array", "three" }) catch unreachable;
    defer alloc.free(doesitwork.str);

    try std.testing.expectEqualSlices(u8, "*0\r\n", empty.str);
    try std.testing.expectEqualSlices(u8, "*2\r\n$4\r\ntest\r\n$5\r\narray\r\n", elements.str);
    try std.testing.expectEqualSlices(u8, "*3\r\n$4\r\ntest\r\n$5\r\narray\r\n$5\r\nthree\r\n", doesitwork.str);
}
