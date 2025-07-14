const std = @import("std");
const print = std.debug.print;

const Broker = @import("broker.zig").Broker;
const tcpServer = @import("tcp_server.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    var broker = Broker.init(allocator);
    defer broker.deinit();
    try tcpServer.run_server(allocator, &broker);
}
