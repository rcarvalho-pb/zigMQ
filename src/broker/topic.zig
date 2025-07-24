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
    messages: ArrayList(*Message),
    consumers: ArrayList(*Consumer),

    pub fn init(allocator: Allocator, name: []const u8) Topic {
        return Topic{
            .allocator = allocator,
            .name = name,
            .messages = ArrayList(*Message).init(allocator),
            .consumers = ArrayList(*Consumer).init(allocator),
        };
    }

    pub fn deinit(self: *@This()) void {
        for (self.messages.items, self.consumers.items) |m, c| {
            self.allocator.destroy(m);
            self.allocator.destroy(c);
        }
        self.messages.deinit();
        self.consumers.deinit();
    }

    pub fn subscribe(self: *@This(), consumer: Consumer) !void {
        const consumerPtr = try self.allocator.create(Consumer);
        consumerPtr.* = consumer;
        try self.consumers.append(consumerPtr);
    }

    pub fn publish(self: *@This(), msg: Message) !void {
        const msgPtr = try self.allocator.create(Message);
        msgPtr.* = msg;
        try self.messages.append(msgPtr);
        for (self.consumers.items) |c| {
            try c.writerFn(c.writer, msg);
        }
    }
};
