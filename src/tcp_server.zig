const std = @import("std");
const net = std.net;
const print = std.debug.print;
const Allocator = std.mem.Allocator;
const Broker = @import("broker.zig").Broker;
const Thread = std.Thread;
const clientHandler = @import("client_handler.zig");

pub fn run_server(allocator: Allocator, broker: *Broker) !void {
    const addr = try net.Address.resolveIp("127.0.0.1", 8080);
    var srv = try addr.listen(.{ .reuse_address = true });
    print("zigMQ started on port: [{s}]\n", .{"8080"});
    while (true) {
        var conn = try srv.accept();
        print("new client connected on port: {d}\n", .{conn.address.getPort()});
        _ = try Thread.spawn(.{}, clientHandler.handle, .{ allocator, broker, conn });
    }
}
