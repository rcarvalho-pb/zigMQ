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
                break;
            }
        }
        return TopicError.ConnectionNotFound;
    }

    pub fn broadcast(self: @This(), msg: []const u8) !void {
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

    pub fn subscribe(self: *@This(), topic: []const u8, conn: *Connection) !void {
        const entry = try self.topics.getOrPut(topic);
        if (!entry.found_existing) {
            entry.value_ptr.* = std.ArrayList(Topic).init(self.allocator);
        }
        try entry.value_ptr.*.add(conn);
    }

    pub fn unsubscribe(self: *@This(), topic: []const u8, conn: *Connection) !void {
        const entry = try self.topics.getOrPut(topic);
        if (!entry.found_existing) {
            return TopicError.TopicNotFound;
        }
        try entry.value_ptr.*.remove(conn);
    }

    pub fn publish(self: @This(), topic: []const u8, msg: []const u8) !void {
        const entry = try self.topics.getOrPut(topic);
        if (!entry.found_existing) {
            return TopicError.TopicNotFound;
        }
        try entry.value_ptr.*.broadcast(msg);
    }
};
