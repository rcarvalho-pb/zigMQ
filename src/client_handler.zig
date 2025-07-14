const std = @import("std");
const print = std.debug.print;
const net = std.net;
const Thread = std.Thread;
const Broker = @import("broker.zig").Broker;
const Message = @import("message.zig");

const Allocator = std.mem.Allocator;
const Connection = net.Server.Connection;

const ClientError = error{
    TopicNotFound,
};

const Client = struct {
    topics: std.ArrayList([]const u8),
    connection: Connection,
    allocator: Allocator,

    pub fn init(allocator: Allocator, connection: Connection) Client {
        return Client{
            .allocator = allocator,
            .connection = connection,
            .topics = std.ArrayList([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *@This()) void {
        self.topics.deinit();
        self.connection.stream.close();
    }

    pub fn addTopic(self: *@This(), topic: []const u8) !void {
        try self.topics.append(topic);
    }

    pub fn removeTopic(self: *@This(), topic: []const u8) !void {
        for (self.topics.items, 0..) |t, i| {
            if (std.ascii.eqlIgnoreCase(topic, t)) {
                _ = self.topics.swapRemove(i);
                return;
            }
        }
        return ClientError.TopicNotFound;
    }
};

pub fn handle(allocator: Allocator, broker: *Broker, connection: Connection) void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    const arena_allocator = arena.allocator();
    var client = Client.init(arena_allocator, connection);
    while (true) {
        var buffer = std.ArrayList(u8).init(arena_allocator);
        defer buffer.deinit();
        var byte: [1]u8 = undefined;
        while (true) {
            const bytes_read = connection.stream.read(&byte) catch |err| {
                print("err reading byte: {}\n", .{err});
                return;
            };

            if (bytes_read == 0) {
                print("cliente disconnected!\n", .{});
                for (client.topics.items) |t| {
                    broker.unsubscribe(t, &client.connection) catch |err| {
                        print("error unsubscribing from topic {s}: {}\n", .{ t, err });
                        return;
                    };
                    client.removeTopic(t) catch |err| {
                        print("error unsubscribing from topic {s}: {}\n", .{ t, err });
                        return;
                    };
                }
            }

            if (byte[0] == '\n') {
                break;
            }

            buffer.append(byte[0]) catch |err| {
                print("error appending byte to buffer: {}\n", .{err});
                return;
            };
            const line = buffer.items;
            const msg = Message.parseMessage(line) catch |err| {
                print("error parsing msg: {}\n", .{err});
                continue;
            };
            print("msg parsed successfully: {}\n", .{msg});
        }
    }
}
