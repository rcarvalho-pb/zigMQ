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

    pub fn subscribe(self: *Self, topic_name: []const u8, consumer: Consumer) !void {
        var topic = try self.getOrCreateTopic(topic_name);
        try topic.subscribe(consumer);
    }

    pub fn publish(self: *Self, topic_name: []const u8, msg: Message) !void {
        const topic = self.getOrCreateTopic(topic_name) catch return BrokerError.TopicNotFound;
        try topic.publish(msg);
    }

    fn getOrCreateTopic(self: *Self, topic_name: []const u8) !*Topic {
        const topic_copy = try self.allocator.dupe(u8, topic_name);
        const entry = try self.topics.getOrPut(topic_copy);
        if (!entry.found_existing) {
            const topicPtr = try self.allocator.create(Topic);
            topicPtr.* = Topic.init(self.allocator, topic_copy);
            entry.value_ptr.* = topicPtr;
        } else {
            self.allocator.free(topic_copy);
        }
        return entry.value_ptr.*;
    }
};
