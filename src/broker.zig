const std = @import("std");
const Print = std.debug.print;

const Allocator = std.mem.Allocator;
const Connection = std.net.Server.Connection;

const ErrorTopic = error{
    NotFound,
    NotRemoved,
};

const Topic = struct {
    subscribers: std.ArrayList(*Connection),
    allocator: Allocator,

    pub fn init(allocator: Allocator) Topic {
        return Topic{
            .allocator = allocator,
            .subscribers = std.ArrayList(*Connection).init(allocator),
        };
    }
    pub fn deinit(self: *@This()) void {
        for (self.subscribers.items) |conn| {
            conn.stream.close();
            self.allocator.destroy(conn);
        }
        self.subscribers.deinit();
    }
    pub fn add(self: *@This(), conn: *Connection) !void {
        const conn_copy = try self.allocator.create(Connection);
        conn_copy.* = conn.*;
        try self.subscribers.append(conn_copy);
    }
    pub fn remove(self: *@This(), conn: *Connection) !void {
        for (self.subscribers.items, 0..) |c, i| {
            if (compareConnection(c, conn)) {
                const removed = self.subscribers.swapRemove(i);
                self.allocator.destroy(removed);
                break;
            }
        }
    }
    pub fn broadcast(self: *@This(), message: []const u8) !void {
        var line = std.ArrayList(u8).init(self.allocator);
        defer line.deinit();
        try line.appendSlice(message);
        try line.append('\n');
        for (self.subscribers.items) |c| {
            try c.stream.writeAll(message);
        }
    }
    pub fn print(self: @This()) void {
        for (self.subscribers.items, 0..) |c, i| {
            Print("\t\t{d} - {}\n", .{ i + 1, c });
        }
    }
    pub fn compareConnection(conn1: *Connection, conn2: *Connection) bool {
        return conn1.stream.handle == conn2.stream.handle and std.net.Address.eql(conn1.address, conn2.address);
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
    pub fn subscribe(self: *@This(), topic: []const u8, subscriber: *Connection) !void {
        const topic_copy = try self.allocator.dupe(u8, topic);
        const entry = try self.topics.getOrPut(topic_copy);
        if (!entry.found_existing) {
            entry.value_ptr.* = Topic.init(self.allocator);
        } else {
            self.allocator.free(topic_copy);
        }
        try entry.value_ptr.add(subscriber);
    }
    pub fn unsubscribe(self: *@This(), topic: []const u8, subscriber: *Connection) !void {
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
    pub fn publish(self: *@This(), topic: []const u8, message: []const u8) !void {
        var entry = self.topics.getEntry(topic) orelse return ErrorTopic.NotFound;
        Print("Publishing on topic: '{s}'\n", .{topic});
        try entry.value_ptr.broadcast(message);
    }
    pub fn print(self: @This()) void {
        var it = self.topics.iterator();
        while (it.next()) |t| {
            Print("\tTopic: {s}\n", .{t.key_ptr.*});
            t.value_ptr.print();
        }
    }
};
