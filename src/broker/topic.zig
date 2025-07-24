const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const Message = @import("message.zig").Message;

pub const Consumer = struct {
    id: []const u8,
    writer: *anyopaque,
    writerFn: *const fn (ctx: *anyopaque, msg: Message) anyerror!void,
};

pub const Topic = struct {
    allocator: Allocator,
    name: []const u8,
    messages: ArrayList(Message),
    consumers: ArrayList(Consumer),

    pub fn init(allocator: Allocator, name: []const u8) Topic {
        return Topic{
            .allocator = allocator,
            .name = name,
            .messages = ArrayList(Message).init(allocator),
            .consumers = ArrayList(Consumer).init(allocator),
        };
    }

    pub fn deinit(self: *@This()) void {
        self.messages.deinit();
        self.consumers.deinit();
    }

    pub fn subscribe(self: *@This(), consumer: Consumer) !void {
        try self.consumers.append(consumer);
    }

    pub fn publish(self: *@This(), msg: Message) !void {
        try self.messages.append(msg);
        for (self.consumers.items) |c| {
            try c.writerFn(c.writer, msg);
        }
    }
};
