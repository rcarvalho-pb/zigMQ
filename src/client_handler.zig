const std = @import("std");
const Print = std.debug.print;

const Broker = @import("broker.zig").Broker;
const ParseMessage = @import("message.zig").parseCommand;
const ParseIP = @import("tcp_server.zig").formatIp;

const Allocator = std.mem.Allocator;
const Connection = std.net.Server.Connection;

pub fn handle(allocator: Allocator, broker: *Broker, connection: Connection) void {
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const arena_allocator = arena.allocator();

    while (true) {
        var buf = std.ArrayList(u8).init(arena_allocator);
        var byte: [1]u8 = undefined;
        while (true) {
            const bytes_read = connection.stream.read(&byte) catch |err| {
                Print("error reading byte: {}\n", .{err});
                return;
            };
            if (bytes_read == 0) {
                var ip_buffer: [40]u8 = undefined;
                Print("Client disconnected from: {s}:{d}\n", .{ ParseIP(connection.address, ip_buffer[0..]), connection.address.getPort() });
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
            return;
        };
        switch (command) {
            .subscribe => |s| {
                Print("Command parsed successfully: Topic {s} - Subscriber {s}\n", .{ s.topic, s.subscriber });
                broker.subscribe(s.topic, s.subscriber) catch |err| {
                    Print("error subscribing on [{s}]: {}\n", .{ s.topic, err });
                    return;
                };
            },
            .unsubscribe => |s| {
                Print("Command parsed successfully: Topic {s} - Subscriber {s}\n", .{ s.topic, s.subscriber });
                broker.unsubscribe(s.topic, s.subscriber) catch |err| {
                    Print("error unsubscribing on [{s}]: {}\n", .{ s.topic, err });
                    return;
                };
            },
        }
        broker.print();
    }
}
