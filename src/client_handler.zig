const std = @import("std");
const Print = std.debug.print;

const Broker = @import("broker.zig").Broker;
const ParseMessage = @import("message.zig").parseCommand;
const ParseIP = @import("tcp_server.zig").formatIp;

const Allocator = std.mem.Allocator;
const Connection = std.net.Server.Connection;

pub fn handle(allocator: Allocator, broker: *Broker, connection: *const Connection) void {
    var conn = connection.*;
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    while (true) {
        var buf = std.ArrayList(u8).init(arena_allocator);
        var byte: [1]u8 = undefined;
        while (true) {
            const bytes_read = conn.stream.read(&byte) catch |err| {
                Print("error reading byte: {}\n", .{err});
                return;
            };
            if (bytes_read == 0) {
                var ip_buffer: [40]u8 = undefined;
                Print("Client disconnected from: {s}:{d}\n", .{ ParseIP(conn.address, ip_buffer[0..]), connection.address.getPort() });
                return;
            }
            if (byte[0] == '\n') {
                break;
            }
            buf.append(byte[0]) catch |err| {
                Print("error appending byte: {}\n", .{err});
                return;
            };
            if (buf.items.len == 0) {
                std.debug.print("Conexão vazia, encerrando.\n", .{});
                break;
            }
        }
        const command = ParseMessage(buf.items) catch |err| {
            Print("error parsing command: {}\n", .{err});
            continue;
        };
        switch (command) {
            .subscribe => |s| {
                Print("Subscribe command parsed successfully: Topic {s}\n", .{s.topic});
                broker.subscribe(s.topic, &conn) catch |err| {
                    Print("error subscribing on [{s}]: {}\n", .{ s.topic, err });
                    continue;
                };
            },
            .unsubscribe => |s| {
                Print("Unsubscribe command parsed successfully: Topic {s}\n", .{s.topic});
                broker.unsubscribe(s.topic, &conn) catch |err| {
                    Print("error unsubscribing on [{s}]: {}\n", .{ s.topic, err });
                    continue;
                };
            },
            .publish => |c| {
                Print("Publish command parsed successfully: Topic {s} - Message {s}\n", .{ c.topic, c.message });
                broker.publish(c.topic, c.message) catch |err| {
                    Print("error publishing message {s} on {s}\n", .{ c.message, c.topic });
                    Print("error: {}\n", .{err});
                    continue;
                };
            },
        }
        broker.print();
    }
}
