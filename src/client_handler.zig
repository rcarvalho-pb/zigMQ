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
        const topic_copy = try self.allocator.alloc(u8, topic.len);
        std.mem.copyForwards(u8, topic_copy, topic);
        try self.topics.append(topic_copy);
    }

    pub fn removeTopic(self: *@This(), topic: []const u8) !void {
        for (self.topics.items, 0..) |t, i| {
            if (std.ascii.eqlIgnoreCase(topic, t)) {
                _ = self.topics.swapRemove(i);
                self.allocator.free(t);
                return;
            }
        }
        return ClientError.TopicNotFound;
    }

    pub fn printKeys(self: @This()) void {
        print("Client:\n", .{});
        for (self.topics.items, 0..) |t, i| {
            print("Index: {d} - Topic: {s}\n", .{ i, t });
        }
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
                return;
            }

            if (byte[0] == '\n') {
                break;
            }

            buffer.append(byte[0]) catch |err| {
                print("error appending byte to buffer: {}\n", .{err});
                return;
            };
        }
        const line = buffer.items;
        const msg = Message.parseMessage(line) catch |err| {
            print("error parsing msg: {}\n", .{err});
            continue;
        };
        switch (msg) {
            .subscribe => |m| {
                print("Subscribe message parsed successfully! Topic: {s}\n", .{m.topic});
                const topic_copy = broker.subscribe(m.topic, &client.connection) catch |err| {
                    print("error subscribing in broker: {}\n", .{err});
                    continue;
                };
                print("Topic copy: {s}\n", .{topic_copy});
                client.addTopic(m.topic) catch |err| {
                    print("error adding topic to client: {}\n", .{err});
                    continue;
                };
                client.printKeys();
                broker.printKeys();
                print("Successfully subscribed to topic: {s}\n", .{m.topic});
            },
            .unsubscribe => |m| {
                print("Unsubscribe message parsed successfully! Topic: {s}\n", .{m.topic});
                broker.unsubscribe(m.topic, &client.connection) catch |err| {
                    print("error unsubscribing from broker topic: {}\n", .{err});
                    continue;
                };
                client.removeTopic(m.topic) catch |err| {
                    print("error unsubscribing from client topics: {}\n", .{err});
                    continue;
                };
                client.printKeys();
                broker.printKeys();
                print("Successfully unsubscribe from topic: {s}\n", .{m.topic});
            },
            .publish => |m| {
                print("Publish message parsed successfully! Topic: {s} - Message: {s}\n", .{ m.topic, m.message });
                var keys = std.ArrayList([]const u8).init(allocator);
                var it = broker.topics.iterator();
                while (it.next()) |entry| {
                    keys.append(entry.key_ptr.*) catch |err| {
                        print("error appending key: {}\n", .{err});
                        break;
                    };
                }
                for (keys.items, 0..) |k, i| {
                    print("key: {s} - pos: {d}\n", .{ k, i });
                }
                broker.publish(m.topic, m.message) catch |err| {
                    print("error publishing on topic {s}: {}\n", .{ m.topic, err });
                    continue;
                };
                client.printKeys();
                broker.printKeys();
                print("Successfully broadcasted message [{s}] on topic [{s}]\n", .{ m.message, m.topic });
            },
        }
    }
}
