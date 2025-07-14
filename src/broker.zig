const std = @import("std");
const print = std.debug.print;
const net = std.net;
const Connection = net.Server.Connection;
const Thread = std.Thread;
const Allocator = std.mem.Allocator;

const TopicError = error{
    TopicNotFound,
    ConnectionNotFound,
};

const Topic = struct {
    subscribers: std.ArrayList(*Connection),
    mutex: Thread.Mutex,
    allocator: Allocator,

    pub fn init(allocator: Allocator) Topic {
        return Topic{
            .allocator = allocator,
            .mutex = Thread.Mutex{},
            .subscribers = std.ArrayList(*Connection).init(allocator),
        };
    }

    pub fn deinit(self: *@This()) void {
        self.subscribers.deinit();
    }

    pub fn add(self: *@This(), conn: *Connection) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.subscribers.append(conn);
    }

    pub fn remove(self: *@This(), conn: *Connection) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.subscribers.items, 0..) |c, i| {
            if (c == conn) {
                _ = self.subscribers.swapRemove(i);
                return;
            }
        }
        return TopicError.ConnectionNotFound;
    }

    pub fn broadcast(self: *@This(), msg: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        for (self.subscribers.items) |c| {
            try c.stream.writeAll(msg);
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
            entry.value_ptr.*.deinit();
        }
        self.topics.deinit();
    }

    pub fn subscribe(self: *@This(), topic: []const u8, conn: *Connection) ![]const u8 {
        const topic_copy = try self.allocator.dupe(u8, topic);
        const entry = try self.topics.getOrPut(topic_copy);
        self.printKeys();
        if (!entry.found_existing) {
            entry.value_ptr.* = Topic.init(self.allocator);
        } else {
            self.allocator.free(topic_copy);
        }
        self.printKeys();
        try entry.value_ptr.*.add(conn);
        self.printKeys();
        return topic_copy;
    }

    pub fn unsubscribe(self: *@This(), topic: []const u8, conn: *Connection) !void {
        var t = self.topics.get(topic) orelse return TopicError.TopicNotFound;
        try t.remove(conn);
        if (t.subscribers.items.len == 0) {
            defer t.deinit();

            var it = self.topics.iterator();
            var key_to_free: ?[]const u8 = null;
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, topic)) {
                    key_to_free = entry.key_ptr.*;
                    break;
                }
            }

            const removed = self.topics.remove(topic);
            if (!removed) return TopicError.TopicNotFound;
            if (key_to_free) |k| {
                self.allocator.free(k);
            }
        }
        self.printKeys();
    }

    pub fn publish(self: @This(), topic: []const u8, msg: []const u8) !void {
        self.printKeys();
        var t = self.topics.get(topic) orelse return TopicError.TopicNotFound;
        try t.broadcast(msg);
    }

    pub fn printKeys(self: @This()) void {
        print("Broker:\n", .{});
        var it = self.topics.iterator();
        var index: u8 = 0;
        while (it.next()) |entry| {
            print("Key: {s} - Index: {d}\n", .{ entry.key_ptr.*, index });
            index += 1;
        }
    }
};
