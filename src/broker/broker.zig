const std = @import("std");

const Topic = @import("topic.zig").Topic;
const PersistenceMode = @import("topic.zig").PersistenceMode;
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

    pub fn createTopic(self: *Self, topic_name: []const u8, persistenceMode: ?PersistenceMode) !*Topic {
        const topic_name_copy = try self.allocator.dupe(u8, topic_name);
        const topicPtr = try self.allocator.create(Topic);
        topicPtr.* = try Topic.init(self.allocator, topic_name_copy, persistenceMode);
        try self.topics.put(topic_name_copy, topicPtr);
        return topicPtr;
    }

    pub fn subscribe(self: *Self, topic_name: []const u8, consumer: Consumer) !void {
        var topic = self.topics.get(topic_name) orelse return BrokerError.TopicNotFound;
        try topic.subscribe(consumer);
    }

    pub fn unsubscribe(self: *Self, topic_name: []const u8, consumer_id: []const u8) !void {
        const topic = self.topics.get(topic_name) orelse return BrokerError.TopicNotFound;
        topic.unsubscribe(consumer_id);
    }

    pub fn publish(self: *Self, topic_name: []const u8, msg: Message) !void {
        const topic = self.topics.get(topic_name) orelse return BrokerError.TopicNotFound;
        try topic.publish(msg);
    }

    pub fn listTopics(self: Self) !std.ArrayList([]const u8) {
        var list = std.ArrayList([]const u8).init(self.allocator);
        var it = self.topics.iterator();
        while (it.next()) |entry| {
            try list.append(entry.key_ptr.*);
        }
        return list;
    }

    pub fn listConsumers(self: Self, topic_name: []const u8) !std.ArrayList([]const u8) {
        const topic = self.topics.get(topic_name) orelse return BrokerError.TopicNotFound;
        const list = try topic.listConsumers();
        return list;
    }

    pub fn replay(self: *Self, topic_name: []const u8, consumer_id: []const u8) !void {
        const topic = self.topics.get(topic_name) orelse return BrokerError.TopicNotFound;
        try topic.replay(consumer_id);
    }
};
