const std = @import("std");
const Print = std.debug.print;

const Allocator = std.mem.Allocator;

const ErrorTopic = error{
    NotFound,
    NotRemoved,
};

const Topic = struct {
    subscribers: std.ArrayList([]const u8),
    allocator: Allocator,

    pub fn init(allocator: Allocator) Topic {
        return Topic{
            .allocator = allocator,
            .subscribers = std.ArrayList([]const u8).init(allocator),
        };
    }
    pub fn deinit(self: *@This()) void {
        for (self.subscribers.items) |sub| {
            self.allocator.free(sub);
        }
        self.subscribers.deinit();
    }
    pub fn add(self: *@This(), sub: []const u8) !void {
        const subscriber = try self.allocator.dupe(u8, sub);
        try self.subscribers.append(subscriber);
    }
    pub fn remove(self: *@This(), sub: []const u8) !void {
        for (self.subscribers.items, 0..) |s, i| {
            if (std.mem.eql(u8, s, sub)) {
                const removed = self.subscribers.swapRemove(i);
                self.allocator.free(removed);
                break;
            }
        }
    }
    pub fn print(self: @This()) void {
        for (self.subscribers.items, 0..) |s, i| {
            Print("\t\t{d} - {s}\n", .{ i + 1, s });
        }
    }
};

pub const Broker = struct {
    topics: std.StringHashMap(Topic),
    allocator: Allocator,

    pub fn init(allocator: Allocator) Broker {
        return Broker{
            .allocator = allocator,
            .topics = std.StringHashMap(Topic).init(allocator),
        };
    }
    pub fn deinit(self: *@This()) void {
        var it = self.topics.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
        }
    }
    pub fn subscribe(self: *@This(), topic: []const u8, subscriber: []const u8) !void {
        const topic_copy = try self.allocator.dupe(u8, topic);
        const entry = try self.topics.getOrPut(topic_copy);
        if (!entry.found_existing) {
            entry.value_ptr.* = Topic.init(self.allocator);
        } else {
            self.allocator.free(topic_copy);
        }
        try entry.value_ptr.add(subscriber);
    }
    pub fn unsubscribe(self: *@This(), topic: []const u8, subscriber: []const u8) !void {
        var entry = self.topics.getEntry(topic) orelse return ErrorTopic.NotFound;
        try entry.value_ptr.remove(subscriber);
        if (entry.value_ptr.subscribers.items.len == 0) {
            const topic_key = entry.key_ptr.*;
            var value_ptr = entry.value_ptr.*;
            if (self.topics.remove(topic_key)) {
                value_ptr.deinit();
                self.allocator.free(topic_key);
            } else return ErrorTopic.NotRemoved;
        }
    }
    pub fn print(self: @This()) void {
        var it = self.topics.iterator();
        while (it.next()) |t| {
            Print("\tTopic: {s}\n", .{t.key_ptr.*});
            t.value_ptr.print();
        }
    }
};
