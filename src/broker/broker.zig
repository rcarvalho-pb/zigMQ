const std = @import("std");

const Topic = @import("topic.zig").Topic;
const Consumer = @import("topic.zig").Consumer;
const Message = @import("message.zig").Message;

const Allocator = std.mem.Allocator;

pub const BrokerError = error{
    TopicNotFound,
};

pub const Broker = struct {
    allocator: Allocator,
    topics: std.StringHashMap(*Topic),

    const Self = @This();

    pub fn init(allocator: Allocator) Broker {
        return Broker{
            .allocator = allocator,
            .topics = std.StringHashMap(*Topic).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.topics.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.topics.deinit();
    }

    pub fn subscribe(self: *Self, topic_name: []const u8, consumer: Consumer) BrokerError!void {
        var topic = try self.getOrCreateTopic(topic_name);
        try topic.subscribe(consumer);
    }

    pub fn publish(self: *Self, topic_name: []const u8, msg: Message) BrokerError!void {
        const topic = try self.getOrCreateTopic(topic_name);
        try topic.publish(msg);
    }

    fn getOrCreateTopic(self: *Self, topic_name: []const u8) !*Topic {
        const topic_name_copy = try self.allocator.dupe(u8, topic_name);
        if (self.topics.get(topic_name_copy)) |t| return t;
        const new_topic = try self.allocator.create(Topic);
        new_topic.* = Topic.init(self.allocator, topic_name_copy);
        try self.topics.put(topic_name_copy, new_topic);
        return new_topic;
    }
};
