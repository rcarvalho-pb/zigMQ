const std = @import("std");

pub const Message = struct {
    topic: []const u8,
    payload: []const u8,
    timestamp: i64,
};
